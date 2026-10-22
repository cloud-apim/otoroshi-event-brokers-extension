package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import org.apache.pekko.stream.scaladsl.Source
import com.cloud.apim.otoroshi.extensions.eventbrokers
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, RedisSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.{ClientOptions, RedisClient, RedisURI, SocketOptions}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.storage.drivers.lettuce.ByteStringRedisCodec
import otoroshi.utils.reactive.ReactiveStreamUtils
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.{JsObject, JsValue, Json}
import reactor.core.publisher.Flux

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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

  private def buildResources(): ClientResources = {
    val computationThreadPoolSize = settings.computationThreadPoolSize
    val ioThreadPoolSize = settings.ioThreadPoolSize
    ClientResources
      .builder()
      .applyOnWithOpt(computationThreadPoolSize)((b, size) => b.computationThreadPoolSize(size))
      .applyOnWithOpt(ioThreadPoolSize)((b, size) => b.ioThreadPoolSize(size))
      .build()
  }

  private val resourcesRef  = new AtomicReference[Option[ClientResources]](None)
  private val clientRef     = new AtomicReference[Option[RedisClient]](None)
  private val connectionRef = new AtomicReference[Option[StatefulRedisPubSubConnection[String, ByteString]]](None)

  private def connection: RedisPubSubReactiveCommands[String, ByteString] =
    connectionRef.get().getOrElse(throw new IllegalStateException("the redis client is not started")).reactive()

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val node = redisUri
    val resources = buildResources()
    resourcesRef.set(Some(resources))
    val client = RedisClient.create(resources, node)
    client.setOptions(ClientOptions.builder().socketOptions(SocketOptions.builder().connectTimeout(java.time.Duration.ofSeconds(10)).build()).build())
    clientRef.set(Some(client))
    connectionRef.set(Some(client.connectPubSub(new ByteStringRedisCodec())))
  }

  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connectionRef.get().getOrElse(throw new IllegalStateException("the redis client is not started")).sync().ping()
    ()
  }

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connectionRef.getAndSet(None).foreach(c => Try(c.close()))
    clientRef.getAndSet(None).foreach(c => Try(c.shutdown(java.time.Duration.ZERO, java.time.Duration.ofSeconds(2))))
    resourcesRef.getAndSet(None).foreach(r => Try(r.shutdown(0, 2, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)))
  }

  // TODO: add options to publish enriched event
  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(using env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val topic = event.vendorMetadata.get("channel")
      .orElse(opts.select("topic").asOpt[String])
      .orElse(opts.select("channel").asOpt[String])
      .orElse(settings.defaultChannel)
      .getOrElse(throw new IllegalArgumentException("no channel: none in the call, and the connection has no default_channel"))
    ReactiveStreamUtils.MonoUtils.toFuture(connection.publish(topic, event.data))
      .map(_ => Right(None): Either[JsValue, Option[JsValue]])
      .recover {
        case e: Throwable => Left(Json.obj("error" -> e.getMessage))
      }
  }

  // TODO: add options to consume enriched event
  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {
    val topic = opts.select("topic").asOpt[String].orElse(opts.select("channel").asOpt[String]).orElse(settings.defaultChannel).getOrElse(throw new IllegalArgumentException("no channel: none in the call, and the connection has no default_channel"))
    connection.subscribe(topic)
    val f: Flux[EventWrapper] = connection.observeChannels().map { cm =>
      eventbrokers.EventWrapper(
        key = None,
        data = cm.getMessage,
        keyContentType = "text",
        dataContentType = "binary",
        headers = Map.empty,
        vendorMetadata = Map.apply[String, String](
          "provider" -> "redis",
          "channel" -> cm.getChannel
        )
      )
    }
    Source.fromPublisher(f)
  }
}