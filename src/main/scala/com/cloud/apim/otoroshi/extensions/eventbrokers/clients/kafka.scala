package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, KafkaSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DescribeClusterOptions}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.pekko.kafka.ConsumerMessage
import org.apache.pekko.kafka.ConsumerMessage.CommittableOffset
import org.apache.pekko.kafka.scaladsl.{Committer, Consumer}
import org.apache.pekko.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{Materializer, QueueOfferResult}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.events.KafkaSettings.kafkaSettings
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, Json}

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.util.control.NonFatal

class KafkaBrokerClient(val config: EventBrokerConnection, settings: KafkaSettings, env: Env) extends EventBrokerClient with BlockingIo {

  private val kconfig = settings.kafkaConfig

  // kafkaSettings may block (it waits for the certificates with mtls): only computed in start
  private lazy val properties: Map[String, String] = kafkaSettings(env, kconfig)

  private lazy val producerSettings = ProducerSettings
    .create(env.analyticsActorSystem, new ByteArraySerializer(), new ByteArraySerializer())
    // send blocks while it looks the topic up: not for a minute, and never on otoroshi's threads
    .withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
    .withProperties(properties)
    .withBootstrapServers(kconfig.servers.mkString(","))

  private lazy val consumerSettings = ConsumerSettings
    .create(env.analyticsActorSystem, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    // once the source stops, the consumer stays up this long for the last commits to go through. it
    // keeps its partitions meanwhile: the default 30 seconds would hold the next subscriber of the group
    .withStopTimeout(2.seconds)
    .withProperties(properties)
    .withBootstrapServers(kconfig.servers.mkString(","))

  private val producerRef = new AtomicReference[Option[Producer[Array[Byte], Array[Byte]]]](None)
  private val adminRef    = new AtomicReference[Option[Admin]](None)

  private def producer: Producer[Array[Byte], Array[Byte]] =
    producerRef.get().getOrElse(throw new IllegalStateException("the kafka client is not started"))

  private def admin: Admin = adminRef.get().getOrElse(throw new IllegalStateException("the kafka client is not started"))

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = true, groups = true, replay = true, keys = true, headers = true)

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    rememberIo(ec)
    val adminProperties = new java.util.Properties()
    properties.foreach((k, v) => adminProperties.put(k, v))
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kconfig.servers.mkString(","))
    adminRef.set(Some(Admin.create(adminProperties)))
    producerRef.set(Some(producerSettings.createKafkaProducer()))
  }.flatMap(_ => healthcheck())

  // the producer connects lazily and silently retries forever: asking the cluster who it is tells
  // whether the brokers can actually be reached
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    admin
      .describeCluster(new DescribeClusterOptions().timeoutMs(10000))
      .clusterId()
      .get(10, TimeUnit.SECONDS)
    ()
  }

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    producerRef.getAndSet(None).foreach(p => Try(p.close(java.time.Duration.ofSeconds(5))))
    adminRef.getAndSet(None).foreach(a => Try(a.close(java.time.Duration.ofSeconds(5))))
  }

  private def noTopic = new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic")

  private def buildHeaders(message: BrokerMessage): Headers =
    new RecordHeaders(message.headers.map((key, value) => new RecordHeader(key, value.byteString.toArray): Header).toArray)

  private def send(event: BrokerMessage, opts: JsObject): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    try {
      val topic     = event.metadata.get("topic").orElse(opts.select("topic").asOpt[String]).orElse(settings.defaultTopic).getOrElse(throw noTopic)
      val key       = event.key.orElse(event.metadata.get("key").map(_.byteString)).orElse(opts.select("key").asOpt[String].map(_.byteString))
      val partition = event.metadata.get("partition").orElse(opts.select("partition").asOpt[String]).map(_.toInt)
      val timestamp = event.metadata.get("timestamp").orElse(opts.select("timestamp").asOpt[String]).map(_.toLong)
      val record    = new ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        partition.map(Integer.valueOf).orNull,
        timestamp.map(java.lang.Long.valueOf).orNull,
        key.map(_.toArray).orNull,
        event.data.toArray,
        buildHeaders(event)
      )
      producer.send(
        record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
            if (exception != null) promise.tryFailure(exception) else promise.trySuccess(metadata)
        }
      )
    } catch {
      case NonFatal(e) => promise.tryFailure(e)
    }
    promise.future
  }

  // send is called in order on one thread: the records keep their order in each partition
  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] =
    Future(events.map(event => send(event, opts)))(using io).flatMap { sent =>
      Future.sequence(sent.map { result =>
        result
          .map { rm =>
            PublishResult.ok(
              Json.obj(
                "topic"                 -> rm.topic(),
                "offset"                -> rm.offset(),
                "partition"             -> rm.partition(),
                "timestamp"             -> rm.timestamp(),
                "serialized_key_size"   -> rm.serializedKeySize(),
                "serialized_value_size" -> rm.serializedValueSize()
              )
            )
          }
          .recover { case NonFatal(e) => PublishResult.failed(e) }
      })
    }

  private def toEvent(rec: ConsumerRecord[Array[Byte], Array[Byte]]): BrokerMessage =
    BrokerMessage(
      key = Option(rec.key()).map(v => ByteString(v)),
      data = Option(rec.value()).map(v => ByteString(v)).getOrElse(ByteString.empty),
      headers = rec.headers().asScala.toSeq.map(header => (header.key(), Option(header.value()).map(v => ByteString(v).utf8String).getOrElse(""))).toMap,
      metadata = Map(
        "provider"              -> "kafka",
        "topic"                 -> rec.topic(),
        "offset"                -> rec.offset().toString,
        "partition"             -> rec.partition().toString,
        "timestamp"             -> rec.timestamp().toString,
        "timestamp_type"        -> rec.timestampType().name,
        "serialized_key_size"   -> rec.serializedKeySize().toString,
        "serialized_value_size" -> rec.serializedValueSize().toString
      )
    )

  /** what to read: whole topics through the group, or explicit partitions from a position */
  private def subscription(topics: Seq[String], spec: SubscriptionSpec)(using ec: ExecutionContext): Future[org.apache.pekko.kafka.Subscription] = {
    val partition = spec.options.select("partition").asOpt[String].map(_.toInt)
    if (spec.from == StartFrom.Latest || spec.from == StartFrom.Earliest) {
      partition match {
        case None    => Future.successful(Subscriptions.topics(topics.toSet))
        case Some(p) => Future.successful(Subscriptions.assignment(topics.map(t => new TopicPartition(t, p)).toSet))
      }
    } else {
      admin.describeTopics(topics.asJava).allTopicNames().toCompletionStage.asScala.map { descriptions =>
        val partitions = descriptions.asScala.toSeq
          .flatMap { (topic, description) =>
            description.partitions().asScala.map(p => new TopicPartition(topic, p.partition()))
          }
          .filter(tp => partition.forall(_ == tp.partition()))
        spec.from match {
          case StartFrom.Offset(offset)  => Subscriptions.assignmentWithOffset(partitions.map(tp => (tp, offset)).toMap)
          case StartFrom.Timestamp(time) => Subscriptions.assignmentOffsetsForTimes(partitions.map(tp => (tp, time)).toMap)
          case _                         => Subscriptions.assignment(partitions.toSet)
        }
      }
    }
  }

  // https://pekko.apache.org/docs/pekko-connectors-kafka/current/consumer.html
  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val topics   = (if (spec.topics.isEmpty) settings.defaultTopic.toSeq else spec.topics).flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty)
    if (topics.isEmpty) Future.failed(noTopic) else subscribeTo(topics, spec)
  }

  // the consumer itself is only created once the source runs, and stopped with it
  private def subscribeTo(topics: Seq[String], spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val offsetTo = spec.options.select("offset_to").asOpt[String].map(_.toLong)
    // without a group, a group of its own: the subscriber gets every event, and nothing it commits matters
    val groupId  = spec.group.getOrElse(s"otoroshi-${UUID.randomUUID()}")
    val local    = consumerSettings
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (spec.from == StartFrom.Earliest) "earliest" else "latest")
      // at most once in a group: committed as read. otherwise only what is acked is committed
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, (spec.group.isDefined && spec.delivery == DeliveryMode.AtMostOnce).toString)
      .applyOnWithOpt(spec.options.select("client_id").asOpt[String])((s, id) => s.withClientId(id))
      .applyOnWithOpt(spec.options.select("group_instance_id").asOpt[String])((s, id) => s.withGroupInstanceId(id))
    val events: Future[Source[ReceivedEvent, ?]] = spec.from match {
      // resuming: right after the positions given, the partitions they do not name from now on
      case StartFrom.Positions(positions) =>
        val resumeAt = (assigned: Set[TopicPartition]) =>
          Future.successful(assigned.flatMap(tp => positions.get(s"${tp.topic()}:${tp.partition()}").map(offset => tp -> (offset + 1L))).toMap)
        val all      = Subscriptions.topics(topics.toSet)
        Future.successful(spec.delivery match {
          case DeliveryMode.AtMostOnce  =>
            Consumer.plainPartitionedManualOffsetSource(local, all, resumeAt).flatMapMerge(1024, _._2).map(rec => ReceivedEvent.unacked(toEvent(rec)))
          case DeliveryMode.AtLeastOnce =>
            committed(Consumer.committablePartitionedManualOffsetSource(local, all, resumeAt).flatMapMerge(1024, _._2))
        })
      case _                              =>
        subscription(topics, spec).map { sub =>
          spec.delivery match {
            case DeliveryMode.AtMostOnce  => Consumer.plainSource(local, sub).map(rec => ReceivedEvent.unacked(toEvent(rec)))
            case DeliveryMode.AtLeastOnce => committed(Consumer.committableSource(local, sub))
          }
        }
    }
    events.map { source =>
      offsetTo match {
        case None     => source
        case Some(to) => source.takeWhile(_.message.metadata.get("offset").exists(_.toLong <= to), false)
      }
    }
  }

  /** at least once: the acks go through a committer, in batches; once the subscription ends, what is left is committed */
  private def committed(messages: Source[ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]], ?])(using env: Env): Source[ReceivedEvent, ?] = {
    given Materializer = env.otoroshiMaterializer
    val (acks, _) = Source
      .queue[CommittableOffset](10000)
      .toMat(Committer.sink(CommitterSettings(env.analyticsActorSystem).withMaxBatch(100).withMaxInterval(1.second)))(Keep.both)
      .run()
    // the consumer stays up a while after the source stops (stop-timeout) to let these commits through
    Subscribed(
      messages.map { message =>
        new ReceivedEvent(
          toEvent(message.record),
          () =>
            acks.offer(message.committableOffset) match {
              case QueueOfferResult.Enqueued => Future.unit
              case other                     => Future.failed(new IllegalStateException(s"could not ack kafka offset ${message.record.offset()}: $other"))
            }
        )
      }
    )(() => acks.complete())
  }
}
