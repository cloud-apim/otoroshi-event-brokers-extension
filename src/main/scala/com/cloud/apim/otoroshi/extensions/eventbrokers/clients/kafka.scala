package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DescribeClusterOptions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.events.KafkaConfig
import otoroshi.events.KafkaSettings.kafkaSettings
import otoroshi.utils.http.MtlsConfig
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.{JsObject, JsValue, Json}

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaBrokerClient(val config: EventBrokerConnection, env: Env) extends EventBrokerClient {

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

  // kafkaSettings may block (it waits for the certificates with mtls): only computed in start
  private lazy val properties: Map[String, String] = kafkaSettings(env, kconfig)

  private lazy val producerSettings = ProducerSettings
    .create(env.analyticsActorSystem, new ByteArraySerializer(), new ByteArraySerializer())
    .withProperties(properties)
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val consumerSettings = ConsumerSettings
    .create(env.analyticsActorSystem, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    .withProperties(properties)
    .withBootstrapServers(kconfig.servers.mkString(","))

  private val producerRef = new AtomicReference[Option[Producer[Array[Byte], Array[Byte]]]](None)
  private val adminRef    = new AtomicReference[Option[Admin]](None)

  private def producer: Producer[Array[Byte], Array[Byte]] =
    producerRef.get().getOrElse(throw new IllegalStateException("the kafka client is not started"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val adminProperties = new java.util.Properties()
    properties.foreach((k, v) => adminProperties.put(k, v))
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kconfig.servers.mkString(","))
    adminRef.set(Some(Admin.create(adminProperties)))
    producerRef.set(Some(producerSettings.createKafkaProducer()))
  }.flatMap(_ => healthcheck())

  // the producer connects lazily and silently retries forever: asking the cluster who it is tells
  // whether the brokers can actually be reached
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    adminRef.get().getOrElse(throw new IllegalStateException("the kafka client is not started"))
      .describeCluster(new DescribeClusterOptions().timeoutMs(10000))
      .clusterId()
      .get(10, TimeUnit.SECONDS)
    ()
  }

  private def buildHeaders(wrapper: EventWrapper): Headers = {
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

  def ppublish(event: EventWrapper, opts: JsObject)(using ec: ExecutionContext): Future[JsValue] = {
    val promise = Promise[RecordMetadata]()
    val topic = event.vendorMetadata.get("topic")
      .orElse(opts.select("topic").asOpt[String])
      .getOrElse(kconfig.topic)
    val key = event.key
      .orElse(event.vendorMetadata.get("key").map(_.byteString))
      .orElse(opts.select("key").asOpt[String].map(_.byteString))
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
        if (key.isEmpty) null else key.get.toArray,
        event.data.toArray,
        buildHeaders(event)
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

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    producerRef.getAndSet(None).foreach(p => Try(p.close(java.time.Duration.ofSeconds(5))))
    adminRef.getAndSet(None).foreach(a => Try(a.close(java.time.Duration.ofSeconds(5))))
  }

  private def callback(promise: Promise[RecordMetadata]) =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        if (exception != null) {
          promise.failure(exception)
        } else {
          promise.success(metadata)
        }

    }

  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(using env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    ppublish(event, opts).map { res =>
      Right(Some(res))
    }
  }

  // https://pekko.apache.org/docs/pekko-connectors-kafka/current/consumer.html
  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {

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
      .applyOnWithOpt(groupIdOpt)((s, groupId) => s.withGroupId(groupId))
      .applyOnWithOpt(clientIdOpt)((s, clientId) => s.withClientId(clientId))
      .applyOnWithOpt(groupInstanceIdOpt)((s, groupInstanceId) => s.withGroupInstanceId(groupInstanceId))
      .applyOnWithOpt(autoOffsetResetOpt)((s, autoOffsetReset) => s.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset))
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
        onRevoke = _ => ()
      )
    }
    val consumer = consumerRaw
      .filter((topicPartition, _) => partitionOpt.forall(_ == topicPartition.partition()))
      .flatMapConcat(_._2)
    consumer
      .takeWhile(rec => offsetToOpt.forall(rec.offset() <= _), false)
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
