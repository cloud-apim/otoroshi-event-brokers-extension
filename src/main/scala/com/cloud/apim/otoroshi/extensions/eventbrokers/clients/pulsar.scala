package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, PulsarAuth, PulsarSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.sksamuel.pulsar4s.{ConsumerConfig, DefaultProducerMessage, DefaultPulsarClient, MessageId, ProducerConfig, PulsarClient, Subscription, Topic, Consumer as PulsarConsumer, Producer as PulsarProducer}
import org.apache.pulsar.client.api.{Authentication, PulsarClientException, Schema, PulsarClient as JPulsarClient}
import org.apache.pulsar.client.impl.auth._
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.{JsNull, JsObject, JsValue, Json}

import java.util.concurrent.{ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object PulsarBroker {

  given byteStringSchema: Schema[ByteString] = new Schema[ByteString] {
    override def clone(): Schema[ByteString] = this
    override def encode(t: ByteString): Array[Byte] = t.toArray
    override def decode(bytes: Array[Byte]): ByteString = if (bytes == null) ByteString.empty else ByteString(bytes)
    override def getSchemaInfo: SchemaInfo = {
      SchemaInfoImpl.builder()
        .name(classOf[ByteString].getCanonicalName)
        .`type`(SchemaType.BYTES)
        .schema(Array.empty[Byte])
        .build()
    }
  }

  /** the native client: pulsar4s wraps it, and the healthcheck needs it */
  def client(_env: otoroshi.env.Env, settings: PulsarSettings): JPulsarClient = {
    // TODO: moare options from config
    val auth: Authentication = settings.auth match {
      case PulsarAuth.Basic(username, password) => new AuthenticationBasic().seffectOn(_.configure(new AuthenticationDataBasic(username, password).getCommandData))
      case PulsarAuth.Token(token) => new AuthenticationToken().seffectOn(_.configure(new AuthenticationDataToken(() => token).getCommandData))
      case PulsarAuth.Disabled => new AuthenticationDisabled()
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

  // TODO: configure based on opts and global config
  def producer(client: PulsarClient, topic: String): PulsarProducer[ByteString] = {
    val producerConfig = ProducerConfig(Topic(topic))
    client.producer[ByteString](producerConfig)
  }

  // TODO: configure based on opts and global config
  def consumer(client: PulsarClient, topic: String): PulsarConsumer[ByteString] = {
    val consumerConfig = ConsumerConfig(topics = Seq(Topic(topic)), subscriptionName = Subscription("otoroshi"))
    client.consumer[ByteString](consumerConfig)
  }
}

class PulsarBrokerClient(val config: EventBrokerConnection, settings: PulsarSettings) extends EventBrokerClient {

  private def topicOf(name: Option[String]): String = settings.fullTopic(
    name.orElse(settings.defaultTopic).getOrElse(throw new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic"))
  )
  private val nativeRef = new AtomicReference[Option[JPulsarClient]](None)
  private val clientRef = new AtomicReference[Option[PulsarClient]](None)

  private def client: PulsarClient = clientRef.get().getOrElse(throw new IllegalStateException("the pulsar client is not started"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val native = PulsarBroker.client(env, settings)
    nativeRef.set(Some(native))
    clientRef.set(Some(new DefaultPulsarClient(native)))
  }.flatMap(_ => healthcheck())

  // building the client connects nothing: looking the default topic up needs the broker (without
  // creating anything on the way). a topic that does not exist yet is an answer of the broker too
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    try {
      nativeRef.get().getOrElse(throw new IllegalStateException("the pulsar client is not started"))
        .getPartitionsForTopic(settings.fullTopic(settings.defaultTopic.getOrElse("otoroshi-healthcheck")), false)
        .get(10, TimeUnit.SECONDS)
      ()
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[PulsarClientException.TopicDoesNotExistException] => ()
    }
  }

  override def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    clientRef.set(None)
    nativeRef.getAndSet(None).foreach(c => Try(c.close()))
  }

  def buildProps(wrapper: EventWrapper): Map[String, String] = {
    wrapper.headers ++ Map("content-type" -> wrapper.dataContentType, "key-content-type" -> wrapper.keyContentType, "timestamp" -> System.currentTimeMillis().toString)
  }

  // TODO: handle all stuff kafka handles
  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(using env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val producer = PulsarBroker.producer(client, topicOf(event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String])))
    producer.sendAsync[Future](DefaultProducerMessage(
      key = event.key.map(_.utf8String),
      value = event.data,
      props = buildProps(event),
    ))
    .map(id => Right(Some(Json.obj("id" -> ByteString(id.bytes).utf8String))): Either[JsValue, Option[JsValue]])
    .recover {
      case e: Throwable => Left(Json.obj("error" -> e.getMessage))
    }
  }

  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {
    // TODO: handle specifi pulsar stuff
    // val messageId = opts.select("from")
    val consumer = PulsarBroker.consumer(client, topicOf(opts.select("topic").asOpt[String]))
    val source = com.sksamuel.pulsar4s.pekko.streams.source(() => consumer, Some(MessageId.latest)) // TODO: handle messageId
    source.map { csm =>
      val contentType = csm.props.get("content-type").getOrElse("binary")
      EventWrapper(
        key = csm.key.map(v => ByteString(v)),
        data = csm.value,
        keyContentType = "text",
        dataContentType = contentType,
        headers = csm.props,
        vendorMetadata = Map.apply[String, String](
          "provider" -> "pulsar",
          "topic" -> csm.topic.name,
          "message_id_bytes" -> ByteString(csm.messageId.bytes).encodeBase64.utf8String,
          "message_id" -> Json.obj(
            "partition_index" -> csm.messageId.partitionIndex.map(_.json).getOrElse(JsNull).asValue,
            "entry_id" -> csm.messageId.entryId.map(_.json).getOrElse(JsNull).asValue,
            "ledger_id" -> csm.messageId.ledgerId.map(_.json).getOrElse(JsNull).asValue,
          ).stringify,
          "event_time" -> csm.eventTime.value.toString,
          "producer_name" -> csm.producerName.name,
          "publish_time" -> csm.publishTime.value.toString,
          "replicated_from" -> csm.replicatedFrom.getOrElse(""),
          "redelivery_count" -> csm.redeliveryCount.toString,
          "schema_version" -> ByteString(csm.schemaVersion).encodeBase64.utf8String,
          "sequence_id" -> csm.sequenceId.value.toString,
        )
      )
    }
  }
}