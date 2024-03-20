package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.sksamuel.pulsar4s.{ConsumerConfig, DefaultPulsarClient, MessageId, ProducerConfig, PulsarClient, PulsarClientConfig, Subscription, Topic, Consumer => PulsarConsumer, Producer => PulsarProducer}
import org.apache.pulsar.client.api.{Authentication, Schema}
import org.apache.pulsar.client.impl.auth.{AuthenticationBasic, AuthenticationDataBasic, AuthenticationDataToken, AuthenticationDisabled, AuthenticationToken}
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import otoroshi.env.Env
import otoroshi.events.{PulsarConfig, PulsarSetting}
import otoroshi.utils.http.MtlsConfig
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue, Json}

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

  def producer(client: PulsarClient, config: PulsarConfig, topicName: String): PulsarProducer[ByteString] = {
    val topic          = Topic(s"persistent://${config.tenant}/${config.namespace}/${topicName}")
    val producerConfig = ProducerConfig(topic)
    client.producer[ByteString](producerConfig)
  }

  def consumer(client: PulsarClient, config: PulsarConfig, topicName: String): PulsarConsumer[ByteString] = {
    val topic          = Topic(s"persistent://${config.tenant}/${config.namespace}/${topicName}")
    val consumerConfig = ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("otoroshi"))
    client.consumer[ByteString](consumerConfig)
  }
}

class PulsarBrokerClient(val config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new PulsarBrokerClient")

  implicit val ec = env.otoroshiExecutionContext

  private val logger = play.api.Logger("RabbitMqBrokerClient")
  private val pconfig = PulsarConfig.format.reads(config.settings).getOrElse(PulsarConfig( // TODO: handle auth !!!!!
    uri = "pulsar://localhost:6650",
    tlsTrustCertsFilePath = None,
    tenant = "tenant",
    namespace = "namespace",
    topic = "test",
    mtlsConfig = MtlsConfig(),
  ))
  private val client = PulsarSetting.client(env, pconfig)

  override def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    client.close()
    ().vfuture
  }

  override def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(pconfig.topic)
    val producer = PulsarBroker.producer(client, pconfig, topic)
    producer.sendAsync(event).map { id =>
      Right(Some(Json.obj("id" -> ByteString(id.bytes).utf8String)))
    }
    .recover {
      case e: Throwable => Left(Json.obj("error" -> e.getMessage))
    }
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(pconfig.topic)
    // val messageId = opts.select("from")
    val consumer = PulsarBroker.consumer(client, pconfig, topic)
    val source = com.sksamuel.pulsar4s.akka.streams.source(() => consumer, Some(MessageId.latest)) // TODO: handle messageId
    source.map { csm =>
      val id = for {
        entry <- csm.messageId.entryId
        ledger <- csm.messageId.ledgerId
        idx <- csm.messageId.partitionIndex
      } yield s"${idx}-${ledger}-${entry}"
      EventWrapper(
        id,
        csm.value
      )
    }
  }
}