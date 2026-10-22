package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, RedisSettings}
import io.lettuce.core.pubsub.{RedisPubSubAdapter, StatefulRedisPubSubConnection}
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.{ClientOptions, RedisClient, RedisURI, SocketOptions}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{Materializer, OverflowStrategy}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.storage.drivers.lettuce.ByteStringRedisCodec
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, Json}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Redis pub/sub: one connection to publish (and to check the server), and one connection of its own
 * per subscription, closed with it. Names with glob characters (`*`, `?`, `[`) are patterns.
 */
class RedisBrokerClient(val config: EventBrokerConnection, settings: RedisSettings) extends EventBrokerClient {

  private def redisUri: RedisURI = {
    val uri = RedisURI.create(settings.uri)
    uri.setStartTls(settings.startTls)
    uri.setVerifyPeer(settings.verifyPeer)
    // on top of what the uri carries: the secrets can come from a vault on their own
    settings.password.foreach(p => settings.username match {
      case Some(u) => uri.setAuthentication(u, p.toCharArray)
      case None    => uri.setAuthentication(p.toCharArray)
    })
    // lettuce waits a minute by default: a broker that does not answer must not hold the manager that long
    uri.setTimeout(java.time.Duration.ofSeconds(10))
    uri
  }

  private def buildResources(): ClientResources =
    ClientResources
      .builder()
      .applyOnWithOpt(settings.ioThreadPoolSize)((b, size) => b.ioThreadPoolSize(size))
      .applyOnWithOpt(settings.computationThreadPoolSize)((b, size) => b.computationThreadPoolSize(size))
      .build()

  private val resourcesRef  = new AtomicReference[Option[ClientResources]](None)
  private val clientRef     = new AtomicReference[Option[RedisClient]](None)
  private val connectionRef = new AtomicReference[Option[StatefulRedisPubSubConnection[String, ByteString]]](None)

  private def client: RedisClient = clientRef.get().getOrElse(throw new IllegalStateException("the redis client is not started"))

  private def connection: StatefulRedisPubSubConnection[String, ByteString] =
    connectionRef.get().getOrElse(throw new IllegalStateException("the redis client is not started"))

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = false, groups = false, replay = false, keys = false, headers = false)

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val resources = buildResources()
    resourcesRef.set(Some(resources))
    val client    = RedisClient.create(resources, redisUri)
    client.setOptions(ClientOptions.builder().socketOptions(SocketOptions.builder().connectTimeout(java.time.Duration.ofSeconds(10)).build()).build())
    clientRef.set(Some(client))
    connectionRef.set(Some(client.connectPubSub(new ByteStringRedisCodec())))
  }

  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connection.sync().ping()
    ()
  }

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connectionRef.getAndSet(None).foreach(c => Try(c.close()))
    clientRef.getAndSet(None).foreach(c => Try(c.shutdown(java.time.Duration.ZERO, java.time.Duration.ofSeconds(2))))
    resourcesRef.getAndSet(None).foreach(r => Try(r.shutdown(0, 2, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)))
  }

  private def noChannel = new IllegalArgumentException("no channel: none in the call, and the connection has no default_channel")

  override def publish(events: Seq[EventWrapper], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] = {
    val commands = connection.async()
    Future.sequence(events.map { event =>
      try {
        val channel = event.vendorMetadata
          .get("channel")
          .orElse(opts.select("channel").asOpt[String])
          .orElse(opts.select("topic").asOpt[String])
          .orElse(settings.defaultChannel)
          .getOrElse(throw noChannel)
        commands
          .publish(channel, event.data)
          .asScala
          // how many subscribers got it: 0 is not an error, nobody was listening
          .map(receivers => PublishResult.ok(Json.obj("channel" -> channel, "receivers" -> receivers.longValue())))
          .recover { case NonFatal(e) => PublishResult.failed(e) }
      } catch {
        case NonFatal(e) => Future.successful(PublishResult.failed(e))
      }
    })
  }

  private def isPattern(name: String): Boolean = name.exists(c => c == '*' || c == '?' || c == '[')

  private def toEvent(channel: String, pattern: Option[String], message: ByteString): ReceivedEvent =
    ReceivedEvent.unacked(
      EventWrapper(
        key = None,
        data = message,
        keyContentType = "text",
        dataContentType = "binary",
        headers = Map.empty,
        vendorMetadata = Map("provider" -> "redis", "channel" -> channel) ++ pattern.map(p => Map("pattern" -> p)).getOrElse(Map.empty)
      )
    )

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val names = if (spec.topics.isEmpty) settings.defaultChannel.toSeq else spec.topics.flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty)
    if (names.isEmpty) Future.failed(noChannel)
    else {
      given Materializer = env.otoroshiMaterializer
      // pub/sub has no flow control: a subscriber too slow loses the oldest messages
      val (queue, source) = Source.queue[ReceivedEvent](1024, OverflowStrategy.dropHead).preMaterialize()
      client.connectPubSubAsync(new ByteStringRedisCodec(), redisUri).asScala.flatMap { subscription =>
        subscription.addListener(new RedisPubSubAdapter[String, ByteString] {
          override def message(channel: String, message: ByteString): Unit = { queue.offer(toEvent(channel, None, message)); () }
          override def message(pattern: String, channel: String, message: ByteString): Unit = { queue.offer(toEvent(channel, Some(pattern), message)); () }
        })
        val (patterns, channels) = names.partition(isPattern)
        val commands             = subscription.async()
        val subscribed           = for {
          _ <- if (channels.isEmpty) Future.unit else commands.subscribe(channels*).asScala
          _ <- if (patterns.isEmpty) Future.unit else commands.psubscribe(patterns*).asScala
        } yield Subscribed(source)(() => { subscription.closeAsync(); queue.complete() })
        subscribed.recoverWith { case NonFatal(e) =>
          subscription.closeAsync()
          Future.failed(e)
        }
      }
    }
  }
}
