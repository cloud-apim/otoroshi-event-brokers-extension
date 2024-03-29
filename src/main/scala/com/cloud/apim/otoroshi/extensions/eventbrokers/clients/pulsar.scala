package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.sksamuel.pulsar4s.{ConsumerConfig, DefaultProducerMessage, DefaultPulsarClient, MessageId, ProducerConfig, ProducerMessage, PulsarClient, PulsarClientConfig, Subscription, Topic, Consumer => PulsarConsumer, Producer => PulsarProducer}
import org.apache.pulsar.client.api.{Authentication, Schema}
import org.apache.pulsar.client.impl.auth._
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import otoroshi.env.Env
import otoroshi.events.PulsarConfig
import otoroshi.utils.http.MtlsConfig
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsNull, JsObject, JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}

object PulsarBroker {

  implicit val byteStringSchema = new Schema[ByteString] {
    override def clone(): Schema[ByteString] = this
    override def encode(t: ByteString): Array[Byte] = t.toArray
    override def decode(bytes: Array[Byte]): ByteString = if (bytes == null) ByteString.empty else ByteString(bytes)
    override def getSchemaInfo: SchemaInfo = {
      SchemaInfoImpl.builder()
        .name(manifest[ByteString].runtimeClass.getCanonicalName)
        .`type`(SchemaType.BYTES)
        .schema(Array.empty[Byte])
        .build()
    }
  }

  def client(_env: otoroshi.env.Env, config: PulsarConfig, rawConfig: JsObject): PulsarClient = {
    if (config.mtlsConfig.mtls) {
      val (_, jks, password) = config.mtlsConfig.toJKS(_env)
      val builder = org.apache.pulsar.client.api.PulsarClient
        .builder()
        .serviceUrl(config.uri)
        .enableTlsHostnameVerification(false)
        .allowTlsInsecureConnection(config.mtlsConfig.trustAll)
        .tlsTrustStoreType("JKS")
        .tlsTrustStorePassword(password)
        .tlsTrustCertsFilePath(jks.getAbsolutePath)
        .useKeyStoreTls(true)
      config.tlsTrustCertsFilePath.foreach(builder.tlsTrustCertsFilePath)
      new DefaultPulsarClient(builder.build())
    } else {
      // TODO: moare options from config
      val authOpt: Option[Authentication] = rawConfig.select("auth").asOpt[JsObject].map { obj =>
        obj.select("kind").asOpt[String].getOrElse("disabled").toLowerCase() match {
          case "basic" => new AuthenticationBasic().seffectOn(_.configure(new AuthenticationDataBasic(
            obj.select("username").as[String],
            obj.select("password").as[String],
          ).getCommandData))
          case "token" => new AuthenticationToken().seffectOn(_.configure(new AuthenticationDataToken(() => obj.select("token").as[String]).getCommandData))
          case _ => new AuthenticationDisabled()
        }
      }
      val c = PulsarClientConfig(
        serviceUrl = config.uri,
        authentication = authOpt,

      )
      PulsarClient(c)
    }
  }

  def producer(client: PulsarClient, config: PulsarConfig, topicName: String, globalConfig: JsValue, connConfig: JsValue): PulsarProducer[ByteString] = {
    val topic          = Topic(s"persistent://${config.tenant}/${config.namespace}/${topicName}")
    // TODO: configure based on opts and global config
    val producerConfig = ProducerConfig(topic)
    client.producer[ByteString](producerConfig)
  }

  def consumer(client: PulsarClient, config: PulsarConfig, topicName: String, globalConfig: JsValue, connConfig: JsValue): PulsarConsumer[ByteString] = {
    val topic          = Topic(s"persistent://${config.tenant}/${config.namespace}/${topicName}")
    // TODO: configure based on opts and global config
    val consumerConfig = ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("otoroshi"))
    client.consumer[ByteString](consumerConfig)
  }
}

class PulsarBrokerClient(val config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  private val pconfig = PulsarConfig.format.reads(config.settings).getOrElse(PulsarConfig( // TODO: handle auth !!!!!
    uri = "pulsar://localhost:6650",
    tlsTrustCertsFilePath = None,
    tenant = "tenant",
    namespace = "namespace",
    topic = "test",
    mtlsConfig = MtlsConfig(),
  ))
  private val client = PulsarBroker.client(env, pconfig, config.settings)

  override def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    client.close()
    ().vfuture
  }

  def buildProps(wrapper: EventWrapper, jsObject: JsObject): Map[String, String] = {

  }

  // TODO: handle all stuff kafka handles
  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val topic = event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String]).getOrElse(pconfig.topic)
    val producer = PulsarBroker.producer(client, pconfig, topic, config.settings, opts)
    producer.sendAsync(DefaultProducerMessage(
      key = event.key.map(_.utf8String),
      value = event.data,
      props = buildProps(event: EventWrapper, opts: JsObject),
    )).map { id =>
      Right(Some(Json.obj("id" -> ByteString(id.bytes).utf8String)))
    }
    .recover {
      case e: Throwable => Left(Json.obj("error" -> e.getMessage))
    }
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    // TODO: handle specifi pulsar stuff
    val topic = opts.select("topic").asOpt[String].getOrElse(pconfig.topic)
    // val messageId = opts.select("from")
    val consumer = PulsarBroker.consumer(client, pconfig, topic, config.settings, opts)
    val source = com.sksamuel.pulsar4s.akka.streams.source(() => consumer, Some(MessageId.latest)) // TODO: handle messageId
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