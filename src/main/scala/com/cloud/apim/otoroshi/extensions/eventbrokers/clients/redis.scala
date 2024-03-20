package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import otoroshi.env.Env
import otoroshi.events.KafkaConfig
import otoroshi.events.KafkaSettings.kafkaSettings
import otoroshi.utils.http.MtlsConfig
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

class RedisBrokerClient(config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new RedisBrokerClient")

  val kconfig = KafkaConfig(
    servers = Seq("127.0.0.1:9093"),
    keyPass = None,
    keystore = None,
    truststore = None,
    sendEvents = true,
    hostValidation = false,
    topic = "topic1",
    mtlsConfig = MtlsConfig(),
    securityProtocol = SecurityProtocol.PLAINTEXT.name,
    saslConfig = None
  )

  implicit val ec = env.analyticsExecutionContext

  lazy val logger = play.api.Logger("KafkaBrokerClient")

  private lazy val producerSettings = ProducerSettings
    .create(env.analyticsActorSystem, new ByteArraySerializer(), new ByteArraySerializer())
    .withProperties(kafkaSettings(env, kconfig))
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val consumerSettings = ConsumerSettings
    .create(env.analyticsActorSystem, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    .withProperties(kafkaSettings(env, kconfig))
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val producer: Producer[Array[Byte], Array[Byte]] = producerSettings.createKafkaProducer

  def ppublish(event: ByteString, opts: JsObject): Future[Done] = {
    val promise = Promise[RecordMetadata]
    val topic = opts.select("topic").asOpt[String].getOrElse(kconfig.topic)
    try {
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, event.toArray), callback(promise))
    } catch {
      case NonFatal(e) =>
        promise.failure(e)
    }
    promise.future.map { _ =>
      Done
    }
  }

  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    producer.close()
    ().vfuture
  }

  private def callback(promise: Promise[RecordMetadata]) =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception) =
        if (exception != null) {
          promise.failure(exception)
        } else {
          promise.success(metadata)
        }

    }

  override def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    ppublish(event, opts).map { _ =>
      Right(None)
    }
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(kconfig.topic)
    val subscription = Subscriptions.topics(topic)
    val consumer = Consumer.plainSource(consumerSettings.withGroupId("foo"), subscription)
    consumer.map { rec =>
      EventWrapper(
        Option(rec.key()).map(v => ByteString(v).utf8String),
        ByteString(rec.value())
      )
    }
  }
}