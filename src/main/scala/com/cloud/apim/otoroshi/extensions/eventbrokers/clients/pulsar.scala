package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, PulsarAuth, PulsarSettings}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.apache.pulsar.client.api.{
  Authentication,
  Consumer,
  Message,
  Producer,
  PulsarClientException,
  Schema,
  SubscriptionInitialPosition,
  SubscriptionMode,
  SubscriptionType,
  PulsarClient as JPulsarClient
}
import org.apache.pulsar.client.impl.auth.*
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, Json}

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ExecutionException, TimeUnit}
import java.util.{Base64, UUID}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.util.control.NonFatal

object PulsarBroker {

  def client(_env: otoroshi.env.Env, settings: PulsarSettings): JPulsarClient = {
    val auth: Authentication = settings.auth match {
      case PulsarAuth.Basic(username, password) =>
        new AuthenticationBasic().seffectOn(_.configure(new AuthenticationDataBasic(username, password).getCommandData))
      case PulsarAuth.Token(token)              => new AuthenticationToken().seffectOn(_.configure(new AuthenticationDataToken(() => token).getCommandData))
      case PulsarAuth.Disabled                  => new AuthenticationDisabled()
    }
    val builder = JPulsarClient.builder().serviceUrl(settings.serviceUrl).authentication(auth)
    if (settings.tls.enabled) {
      val mtls = settings.tls.mtlsConfig
      builder
        .enableTlsHostnameVerification(settings.tls.hostnameVerification)
        .allowTlsInsecureConnection(settings.tls.trustAll)
      if (settings.tls.clientCerts.nonEmpty || settings.tls.trustedCerts.nonEmpty) {
        val (_, jks, password) = mtls.toJKS(using _env)
        builder
          .tlsTrustStoreType("JKS")
          .tlsTrustStorePassword(password)
          .tlsTrustCertsFilePath(jks.getAbsolutePath)
          .useKeyStoreTls(true)
      }
      settings.tlsTrustCertsFilePath.foreach(builder.tlsTrustCertsFilePath)
    }
    builder.build()
  }
}

/**
 * Producers are kept, one per topic (the least recently used one goes beyond `maxProducers`).
 * Subscriptions without a group are non durable and named for themselves: every subscriber gets
 * every message, and nothing is left on the broker. With a group, the subscription is durable and
 * shared by default: its subscribers share the messages.
 */
class PulsarBrokerClient(val config: EventBrokerConnection, settings: PulsarSettings) extends EventBrokerClient {

  private val maxProducers = 64

  private val nativeRef = new AtomicReference[Option[JPulsarClient]](None)

  private val producers = new java.util.LinkedHashMap[String, Future[Producer[Array[Byte]]]](16, 0.75f, true)

  private def native: JPulsarClient = nativeRef.get().getOrElse(throw new IllegalStateException("the pulsar client is not started"))

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = true, groups = true, replay = true, keys = true, headers = true)

  private def noTopic = new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic")

  private def topicOf(name: Option[String]): String = settings.fullTopic(name.orElse(settings.defaultTopic).getOrElse(throw noTopic))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    nativeRef.set(Some(PulsarBroker.client(env, settings)))
  }.flatMap(_ => healthcheck())

  // building the client connects nothing: looking the default topic up needs the broker (without
  // creating anything on the way). a topic that does not exist yet is an answer of the broker too
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    try {
      native.getPartitionsForTopic(settings.fullTopic(settings.defaultTopic.getOrElse("otoroshi-healthcheck")), false).get(10, TimeUnit.SECONDS)
      ()
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[PulsarClientException.TopicDoesNotExistException] => ()
    }
  }

  override def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    producers.synchronized {
      producers.values().asScala.foreach(_.foreach(p => Try(p.closeAsync())))
      producers.clear()
    }
    nativeRef.getAndSet(None).foreach(c => Try(c.close()))
  }

  /** the producer of the topic, created on first use */
  private def producer(topic: String)(using ec: ExecutionContext): Future[Producer[Array[Byte]]] = producers.synchronized {
    Option(producers.get(topic)).getOrElse {
      val created = native.newProducer(Schema.BYTES).topic(topic).blockIfQueueFull(true).createAsync().asScala
      producers.put(topic, created)
      // a producer that could not be created is not kept
      created.failed.foreach(_ => producers.synchronized(if (producers.get(topic) eq created) producers.remove(topic)))
      if (producers.size() > maxProducers) {
        val eldest = producers.entrySet().iterator().next()
        producers.remove(eldest.getKey)
        eldest.getValue.foreach(p => p.closeAsync())
      }
      created
    }
  }

  private def buildProps(wrapper: EventWrapper): Map[String, String] =
    wrapper.headers ++ Map(
      "content-type"     -> wrapper.dataContentType,
      "key-content-type" -> wrapper.keyContentType,
      "timestamp"        -> System.currentTimeMillis().toString
    )

  override def publish(events: Seq[EventWrapper], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] = {
    val targets = events.map(event => Try(topicOf(event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String]))))
    // the producers first, then every send in order: messages to one topic keep their order
    Future
      .traverse(targets.flatMap(_.toOption).distinct)(topic => producer(topic).map(p => Try(p)).map(p => (topic, p)))
      .map(_.toMap)
      .flatMap { producersOf =>
        Future.sequence(events.zip(targets).map {
          case (_, scala.util.Failure(e))         => Future.successful(PublishResult.failed(e))
          case (event, scala.util.Success(topic)) =>
            producersOf(topic) match {
              case scala.util.Failure(e) => Future.successful(PublishResult.failed(e))
              case scala.util.Success(p) =>
                try {
                  p.newMessage()
                    .value(event.data.toArray)
                    .applyOnWithOpt(event.key.map(_.utf8String))((m, key) => m.key(key))
                    .properties(buildProps(event).asJava)
                    .sendAsync()
                    .asScala
                    .map { id =>
                      PublishResult.ok(
                        Json.obj("topic" -> topic, "message_id" -> id.toString, "message_id_bytes" -> Base64.getEncoder.encodeToString(id.toByteArray))
                      )
                    }
                    .recover { case NonFatal(e) => PublishResult.failed(e) }
                } catch {
                  case NonFatal(e) => Future.successful(PublishResult.failed(e))
                }
            }
        })
      }
  }

  private def toEvent(message: Message[Array[Byte]]): EventWrapper = {
    val props       = Option(message.getProperties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    val contentType = props.getOrElse("content-type", "binary")
    def some(name: String, value: => Any): Option[(String, String)] = Option(value).map(v => (name, v.toString))
    EventWrapper(
      key = Option(message.getKey).map(ByteString(_)),
      data = Option(message.getValue).map(ByteString(_)).getOrElse(ByteString.empty),
      keyContentType = "text",
      dataContentType = contentType,
      headers = props,
      vendorMetadata = Map("provider" -> "pulsar") ++ Seq(
        some("topic", message.getTopicName),
        some("message_id", message.getMessageId),
        some("message_id_bytes", Base64.getEncoder.encodeToString(message.getMessageId.toByteArray)),
        some("event_time", message.getEventTime),
        some("publish_time", message.getPublishTime),
        some("producer_name", message.getProducerName),
        some("sequence_id", message.getSequenceId),
        some("redelivery_count", message.getRedeliveryCount),
        some("replicated_from", message.getReplicatedFrom),
        // null without a schema on the topic
        some("schema_version", Option(message.getSchemaVersion).map(v => Base64.getEncoder.encodeToString(v)).orNull)
      ).flatten
    )
  }

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val topics = Try(if (spec.topics.isEmpty) Seq(topicOf(None)) else spec.topics.map(t => settings.fullTopic(t)))
    val kind   = spec.options.select("subscription_type").asOpt[String].map(_.toLowerCase)
    val tpe    = kind match {
      case Some("exclusive")  => Some(SubscriptionType.Exclusive)
      case Some("shared")     => Some(SubscriptionType.Shared)
      case Some("failover")   => Some(SubscriptionType.Failover)
      case Some("key_shared") => Some(SubscriptionType.Key_Shared)
      case None               => Some(if (spec.group.isDefined) SubscriptionType.Shared else SubscriptionType.Exclusive)
      case Some(_)            => None
    }
    (topics, tpe) match {
      case (scala.util.Failure(e), _) => Future.failed(e)
      case (_, None)                  => Future.failed(new IllegalArgumentException("subscription_type is one of exclusive, shared, failover, key_shared"))
      case (scala.util.Success(names), Some(subscriptionType)) =>
        native
          .newConsumer(Schema.BYTES)
          .topics(names.asJava)
          .subscriptionName(spec.group.getOrElse(s"otoroshi-${UUID.randomUUID()}"))
          .subscriptionType(subscriptionType)
          .subscriptionMode(if (spec.group.isDefined) SubscriptionMode.Durable else SubscriptionMode.NonDurable)
          .subscriptionInitialPosition(if (spec.from == StartFrom.Earliest) SubscriptionInitialPosition.Earliest else SubscriptionInitialPosition.Latest)
          .receiverQueueSize(100)
          .subscribeAsync()
          .asScala
          .flatMap { consumer =>
            val positioned = spec.from match {
              case StartFrom.Timestamp(time) => consumer.seekAsync(time).asScala
              case StartFrom.Offset(_)       => Future.failed(new IllegalArgumentException("pulsar subscriptions start from earliest, latest or a timestamp"))
              case _                         => Future.unit
            }
            positioned
              .recoverWith { case NonFatal(e) => consumer.closeAsync().asScala.transform(_ => scala.util.Failure(e)) }
              .map(_ => events(consumer, spec.delivery))
          }
    }
  }

  private def events(consumer: Consumer[Array[Byte]], delivery: DeliveryMode)(using env: Env, ec: ExecutionContext): Source[ReceivedEvent, ?] = {
    def ack(message: Message[Array[Byte]]): Future[Unit] = consumer.acknowledgeAsync(message.getMessageId).asScala.map(_ => ())
    val messages = Source.repeat(()).mapAsync(1)(_ => consumer.receiveAsync().asScala)
    val received = delivery match {
      case DeliveryMode.AtMostOnce  => messages.map { message => ack(message); ReceivedEvent.unacked(toEvent(message)) }
      // what is not acked is delivered again once the consumer is gone
      case DeliveryMode.AtLeastOnce => messages.map(message => new ReceivedEvent(toEvent(message), () => ack(message)))
    }
    Subscribed(received)(() => { consumer.closeAsync(); () })
  }
}
