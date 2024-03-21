package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import otoroshi.env.Env
import otoroshi.events.KafkaConfig
import otoroshi.events.KafkaSettings.kafkaSettings
import otoroshi.utils.http.MtlsConfig
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue, Json}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

class KafkaBrokerClient(val config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new KafkaBrokerClient")

  val kconfig = KafkaConfig.format.reads(config.settings).getOrElse(KafkaConfig(
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
  ))

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

  def ppublish(event: ByteString, opts: JsObject): Future[JsValue] = {
    val promise = Promise[RecordMetadata]
    val topic = opts.select("topic").asOpt[String].getOrElse(kconfig.topic)
    val key = opts.select("key").asOpt[String]
    val partition = opts.select("partition").asOpt[String].map(_.toInt)
    val timestamp = opts.select("timestamp").asOpt[String].map(_.toLong)
    try {
      val pr = key match {
        case None => new ProducerRecord[Array[Byte], Array[Byte]](topic, event.toArray)
        case Some(key) => new ProducerRecord[Array[Byte], Array[Byte]](
          topic,
          if (partition.isEmpty) null else partition.get,
          if (timestamp.isEmpty) null else timestamp.get,
          key.getBytes(StandardCharsets.UTF_8),
          event.toArray,
          null
        )
      }
      producer.send(pr, callback(promise))
    } catch {
      case NonFatal(e) =>
        promise.failure(e)
    }
    promise.future.map { rm =>
      Json.obj(
        "topic" -> rm.topic(),
        "offset" -> rm.offset(),
        "partition" -> rm.partition(),
        "timestamp" -> rm.timestamp(),
        "serialized_key_size" -> rm.serializedKeySize(),
        "serialized_value_size" -> rm.serializedValueSize(),
      )
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
    ppublish(event, opts).map { res =>
      Right(Some(res))
    }
  }

  // https://doc.akka.io/docs/alpakka-kafka/2.0.7/consumer.html
  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(kconfig.topic)
    val subscription = Subscriptions.topics(topic)
    val groupId = opts.select("group_id").asOpt[String].orElse(config.settings.select("group_id").asOpt[String]).getOrElse(UUID.randomUUID().toString)
    val partitionOpt = opts.select("partition").asOpt[String].orElse(config.settings.select("partition").asOpt[String]).map(_.toInt)
    val clientIdOpt = opts.select("client_id").asOpt[String].orElse(config.settings.select("client_id").asOpt[String])
    val groupInstanceIdOpt = opts.select("group_instance_id").asOpt[String].orElse(config.settings.select("group_instance_id").asOpt[String])

    val localSettings = consumerSettings
      .withGroupId(groupId)
      .applyOnWithOpt(clientIdOpt) { case (s, clientId) => s.withClientId(clientId) }
      .applyOnWithOpt(groupInstanceIdOpt) { case (s, groupInstanceId) => s.withGroupInstanceId(groupInstanceId) }
      //.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // TODO: from offset
      // TODO: to offset
      // TODO: commit

    // val consumer = Consumer.plainSource(localSettings, subscription)
    val consumer = Consumer.plainPartitionedSource(localSettings, subscription)
      .filter {
        case (topicPartition, _) if partitionOpt.isEmpty => true
        case (topicPartition, _) if partitionOpt.nonEmpty => partitionOpt.contains(topicPartition.partition())
      }
      .flatMapConcat(_._2)
    consumer.map { rec =>
      EventWrapper(
        Option(rec.key()).map(v => ByteString(v).utf8String),
        ByteString(rec.value())
      )
    }
  }
}