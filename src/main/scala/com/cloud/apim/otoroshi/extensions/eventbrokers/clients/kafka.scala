package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
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

  // TODO: handle post of Json bodies with kafka vnd format, so we could extract keys
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

  // lazy val logger = play.api.Logger("KafkaBrokerClient")

  private lazy val producerSettings = ProducerSettings
    .create(env.analyticsActorSystem, new ByteArraySerializer(), new ByteArraySerializer())
    .withProperties(kafkaSettings(env, kconfig))
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val consumerSettings = ConsumerSettings
    .create(env.analyticsActorSystem, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    .withProperties(kafkaSettings(env, kconfig))
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val producer: Producer[Array[Byte], Array[Byte]] = producerSettings.createKafkaProducer

  private def buildHeaders(wrapper: EventWrapper, opts: JsObject): Headers = {
    val headers = wrapper.headers ++ Map(
      "content-type" -> wrapper.dataContentType,
      "key-content-type" -> wrapper.keyContentType,
      "timestamp" -> System.currentTimeMillis().toString
    )
    val hds: Array[Header] = headers.map {
      case (key, value) => new RecordHeader(key, value.byteString.toArray)
    }.toArray
    new RecordHeaders(hds)
  }

  def ppublish(event: EventWrapper, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[JsValue] = {
    val promise = Promise[RecordMetadata]
    val topic = event.vendorMetadata.get("topic")
      .orElse(opts.select("topic").asOpt[String])
      .getOrElse(kconfig.topic)
    val key = event.vendorMetadata.get("key")
      .orElse(opts.select("key").asOpt[String])
    val partition = event.vendorMetadata.get("partition")
      .orElse(opts.select("partition").asOpt[String])
      .map(_.toInt)
    val timestamp = event.vendorMetadata.get("timestamp")
      .orElse(opts.select("timestamp").asOpt[String])
      .map(_.toLong)
    try {
      val pr = new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        if (partition.isEmpty) null else partition.get,
        if (timestamp.isEmpty) null else timestamp.get,
        if (key.isEmpty) null else key.get.getBytes(StandardCharsets.UTF_8),
        event.data.toArray,
        buildHeaders(event, opts)
      )
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

  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    ppublish(event, opts).map { res =>
      Right(Some(res))
    }
  }

  // https://doc.akka.io/docs/alpakka-kafka/2.0.7/consumer.html
  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {

    import scala.jdk.CollectionConverters._

    val topics = opts.select("topics").asOpt[String].map(_.split(",").map(_.trim).toSeq).getOrElse(Seq.empty) ++ Seq(opts.select("topic").asOpt[String].getOrElse(kconfig.topic)).flatMap(_.split(",").map(_.trim))
    val offsetFromOpt = opts.select("offset_from").asOpt[String].orElse(config.settings.select("offset_from").asOpt[String]).map(_.toLong)
    val offsetToOpt = opts.select("offset_to").asOpt[String].orElse(config.settings.select("offset_to").asOpt[String]).map(_.toLong)
    val groupIdOpt = opts.select("group_id").asOpt[String].orElse(config.settings.select("group_id").asOpt[String]).getOrElse(UUID.randomUUID().toString).some
    val partitionOpt = opts.select("partition").asOpt[String].orElse(config.settings.select("partition").asOpt[String]).map(_.toInt)
    val clientIdOpt = opts.select("client_id").asOpt[String].orElse(config.settings.select("client_id").asOpt[String])
    val groupInstanceIdOpt = opts.select("group_instance_id").asOpt[String].orElse(config.settings.select("group_instance_id").asOpt[String])
    val autoOffsetResetOpt = opts.select("auto_offset_reset").asOpt[String].orElse(config.settings.select("auto_offset_reset").asOpt[String]) // earliest, latest, none

    val localSettings = consumerSettings
      .applyOnWithOpt(groupIdOpt) { case (s, groupId) => s.withGroupId(groupId) }
      .applyOnWithOpt(clientIdOpt) { case (s, clientId) => s.withClientId(clientId) }
      .applyOnWithOpt(groupInstanceIdOpt) { case (s, groupInstanceId) => s.withGroupInstanceId(groupInstanceId) }
      .applyOnWithOpt(autoOffsetResetOpt) { case (s, autoOffsetReset) => s.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset) }
      .applyOnIf(groupIdOpt.isEmpty)(_.withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
      // TODO: delivery

    val consumerRaw = offsetFromOpt match {
      case None => Consumer.plainPartitionedSource(localSettings, Subscriptions.topics(topics.toSet.asJava))
      case Some(offset) => Consumer.plainPartitionedManualOffsetSource(localSettings, Subscriptions.topics(topics.toSet.asJava),
        getOffsetsOnAssign = partitions => {
          val map: Map[TopicPartition, Long] = partitionOpt match {
            case None => partitions.map(p => (p, offset)).toMap
            case Some(part) => partitions
              .filter(p => topics.contains(p.topic()))
              .map(p => (new TopicPartition(p.topic(), part), offset))
              .toMap
          }
          map.vfuture
        },
        onRevoke = partitions => ()
      )
    }
    val consumer = consumerRaw.filter {
        case (_, _) if partitionOpt.isEmpty => true
        case (topicPartition, _) if partitionOpt.nonEmpty => partitionOpt.contains(topicPartition.partition())
      }
      .flatMapConcat(_._2)
    consumer
      .takeWhile({
        case _ if offsetToOpt.isEmpty => true
        case r => r.offset() <= offsetToOpt.get
      }, false)
      .map { rec =>
        val contentType = rec.headers().headers("content-type").asScala.headOption.map(v => ByteString(v.value()).utf8String).getOrElse("binary")
        EventWrapper(
          key = Option(rec.key()).map(v => ByteString(v)),
          data = ByteString(rec.value()),
          keyContentType = "binary",
          dataContentType = contentType,
          headers = rec.headers().asScala.toSeq.map { header =>
            (header.key(), ByteString(header.value()).utf8String)
          }.toMap,
          vendorMetadata = Map(
            "provider" -> "kafka",
            "topic" -> rec.topic(),
            "offset" -> rec.offset().toString,
            "partition" -> rec.partition().toString,
            "timestamp" -> rec.timestamp().toString,
            "timestamp_type" -> rec.timestampType().name,
            "serialized_key_size" -> rec.serializedKeySize().toString,
            "serialized_value_size" -> rec.serializedValueSize().toString,
          )
        )
      }
  }
}