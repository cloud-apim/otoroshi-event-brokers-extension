package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, RabbitMQSettings}
import com.rabbitmq.client.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsBoolean, JsNumber, JsObject, JsString, JsValue, Json}

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Publishing goes through one channel with publisher confirms: a publish is done once the broker has
 * taken the message, and published `mandatory`, so a message no queue gets is reported instead of
 * silently dropped. Every subscription gets a channel of its own, closed with it.
 */
class RabbitMqBrokerClient(val config: EventBrokerConnection, settings: RabbitMQSettings) extends EventBrokerClient with BlockingIo {

  private val factory = new ConnectionFactory()
  factory.setVirtualHost(settings.virtualHost)
  factory.setTopologyRecoveryEnabled(false)
  factory.setAutomaticRecoveryEnabled(true)
  factory.setNetworkRecoveryInterval(1000)
  settings.username.foreach(v => factory.setUsername(v))
  settings.password.foreach(v => factory.setPassword(v))
  factory.setConnectionTimeout(10000)

  private val publishIdHeader = "x-otoroshi-publish-id"

  private val connectionRef = new AtomicReference[Option[Connection]](None)
  private val publisherRef  = new AtomicReference[Option[Channel]](None)

  /** the publishes waiting for their confirm, by sequence number */
  private val pending  = new ConcurrentSkipListMap[java.lang.Long, (String, Promise[Unit])]()
  /** the publishes the broker returned: no queue got them */
  private val returned = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

  // replay: what a stream holds (a queue of the stream type), the other queues refuse it
  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = true, groups = false, replay = true, keys = false, headers = true)

  private def javaValue(value: JsValue): AnyRef = value match {
    case JsString(s)                     => s
    case JsNumber(n) if n.isValidLong    => java.lang.Long.valueOf(n.toLong)
    case JsNumber(n)                     => java.lang.Double.valueOf(n.toDouble)
    case JsBoolean(b)                    => java.lang.Boolean.valueOf(b)
    case other                           => Json.stringify(other)
  }

  private def arguments(o: JsObject): java.util.Map[String, AnyRef] = o.value.view.mapValues(javaValue).toMap.asJava

  /** the topology of the connection, declared (or found the same) before anything else */
  private def declare(connection: Connection): Unit = {
    // one channel per declaration: the broker closes the channel of a declaration it refuses
    def withChannel(what: String)(f: Channel => Unit): Unit = {
      val channel = connection.createChannel()
      try f(channel)
      // not chained: the status of a connection says the innermost message, which says nothing of what was declared
      catch { case NonFatal(e) => throw new IllegalStateException(s"could not declare $what: ${ConnectionManager.describe(e)}") }
      finally Try(if (channel.isOpen) channel.close())
    }
    val topology = settings.topology
    topology.exchanges.foreach { e =>
      withChannel(s"the exchange '${e.name}'")(_.exchangeDeclare(e.name, e.`type`, e.durable, e.autoDelete, false, arguments(e.arguments)))
    }
    topology.queues.foreach { q =>
      val args = if (q.`type` == "classic") q.arguments else q.arguments ++ Json.obj("x-queue-type" -> q.`type`)
      withChannel(s"the queue '${q.name}'")(_.queueDeclare(q.name, q.durable, false, q.autoDelete, arguments(args)))
    }
    topology.bindings.foreach { b =>
      withChannel(s"the binding of '${b.queue}' to '${b.exchange}'")(_.queueBind(b.queue, b.exchange, b.routingKey, arguments(b.arguments)))
    }
  }

  private def connection: Connection = connectionRef.get().getOrElse(throw new IllegalStateException("the rabbitmq client is not started"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    rememberIo(ec)
    if (settings.tls.enabled) {
      // the certificates of the store, in memory: none of them missing, or the start fails (and is tried again)
      factory.useSslProtocol(TlsMaterial.resolve(settings.tls).sslContext)
      if (settings.tls.hostnameVerification && !settings.tls.trustAll) factory.enableHostnameVerification()
    }
    val connection = factory.newConnection(settings.addresses.map(s => Address.parseAddress(s)).toArray)
    try declare(connection)
    catch {
      case NonFatal(e) =>
        Try(connection.close())
        throw e
    }
    connectionRef.set(Some(connection))
    publisherRef.set(Some(newPublisher(connection)))
  }

  // the connection recovers by itself after a network failure. the publishing channel does not survive
  // a protocol error, but it is opened again on the next publish
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] =
    if (!connectionRef.get().exists(_.isOpen)) Future.failed(new IllegalStateException("the rabbitmq connection is closed"))
    else Future.unit

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    publisherRef.getAndSet(None).foreach(c => Try(if (c.isOpen) c.close()))
    connectionRef.getAndSet(None).foreach(c => Try(c.close(5000)))
    failPending(new IllegalStateException("the rabbitmq client is closed"))
  }

  private def failPending(e: Throwable): Unit = {
    pending.values().asScala.foreach(_._2.tryFailure(e))
    pending.clear()
  }

  private def newPublisher(connection: Connection): Channel = {
    val channel = connection.createChannel()
    channel.confirmSelect()
    channel.addReturnListener(new ReturnListener {
      override def handleReturn(code: Int, text: String, exchange: String, routingKey: String, props: AMQP.BasicProperties, body: Array[Byte]): Unit =
        Option(props.getHeaders).flatMap(h => Option(h.get(publishIdHeader))).foreach(id => returned.add(id.toString))
    })
    channel.addConfirmListener(new ConfirmListener {
      // a return always comes before the confirm of the same message
      override def handleAck(tag: Long, multiple: Boolean): Unit = confirm(tag, multiple, nacked = false)
      override def handleNack(tag: Long, multiple: Boolean): Unit = confirm(tag, multiple, nacked = true)
    })
    channel.addShutdownListener(new ShutdownListener {
      override def shutdownCompleted(cause: ShutdownSignalException): Unit = failPending(cause)
    })
    channel
  }

  private def confirm(tag: Long, multiple: Boolean, nacked: Boolean): Unit = {
    val confirmed = if (multiple) pending.headMap(tag, true) else pending.subMap(tag, true, tag, true)
    confirmed.values().asScala.foreach { (id, promise) =>
      if (returned.remove(id)) promise.tryFailure(new IllegalArgumentException("no queue got the message: the queue, or the binding, does not exist"))
      else if (nacked) promise.tryFailure(new IllegalStateException("the broker refused the message"))
      else promise.trySuccess(())
    }
    confirmed.clear()
  }

  /** the publishing channel, opened again when a protocol error closed it */
  private def publisher: Channel = synchronized {
    publisherRef.get().filter(_.isOpen).getOrElse {
      val channel = newPublisher(connection)
      publisherRef.set(Some(channel))
      channel
    }
  }

  /** the amqp properties: the same snake_case names as what is read back (`content_type`, `message_id`, ...) */
  private def buildProps(message: BrokerMessage, publishId: String): AMQP.BasicProperties = {
    val headers: java.util.Map[String, AnyRef] = (message.headers ++ Map(publishIdHeader -> publishId)).view.mapValues(_.asInstanceOf[AnyRef]).toMap.asJava
    val m                                      = message.metadata
    new AMQP.BasicProperties.Builder()
      .headers(headers)
      .applyOnWithOpt(m.get("content_type")) { case (builder, value) => builder.contentType(value) }
      .applyOnWithOpt(m.get("content_encoding")) { case (builder, value) => builder.contentEncoding(value) }
      .applyOnWithOpt(m.get("delivery_mode")) { case (builder, value) => builder.deliveryMode(value.toInt) }
      .applyOnWithOpt(m.get("priority")) { case (builder, value) => builder.priority(value.toInt) }
      .applyOnWithOpt(m.get("correlation_id")) { case (builder, value) => builder.correlationId(value) }
      .applyOnWithOpt(m.get("reply_to")) { case (builder, value) => builder.replyTo(value) }
      .applyOnWithOpt(m.get("expiration")) { case (builder, value) => builder.expiration(value) }
      .applyOnWithOpt(m.get("message_id").orElse(message.key.map(_.utf8String))) { case (builder, value) => builder.messageId(value) }
      .applyOnWithOpt(m.get("timestamp")) { case (builder, value) => builder.timestamp(new Date(value.toLong)) }
      .applyOnWithOpt(m.get("type")) { case (builder, value) => builder.`type`(value) }
      .applyOnWithOpt(m.get("user_id")) { case (builder, value) => builder.userId(value) }
      .applyOnWithOpt(m.get("app_id")) { case (builder, value) => builder.appId(value) }
      .applyOnWithOpt(m.get("cluster_id")) { case (builder, value) => builder.clusterId(value) }
      .build()
  }

  private def noQueue = new IllegalArgumentException("no queue: none in the call, and the connection has no default_queue")

  /** an exchange and its routing key, or a queue through the default exchange */
  private def target(event: BrokerMessage, opts: JsObject): (String, String) = {
    val exchange   = event.metadata.get("exchange").orElse(opts.select("exchange").asOpt[String]).getOrElse("")
    val routingKey = event.metadata.get("routing_key").orElse(opts.select("routing_key").asOpt[String])
    if (exchange.isEmpty) {
      val queue = event.metadata.get("queue").orElse(opts.select("queue").asOpt[String]).orElse(routingKey).orElse(settings.defaultQueue).getOrElse(throw noQueue)
      ("", queue)
    } else {
      (exchange, routingKey.getOrElse(""))
    }
  }

  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] =
    Future {
      // one publisher channel: publishes are serialized on it, which keeps their order
      synchronized {
        events.map { event =>
          try {
            val (exchange, routingKey) = target(event, opts)
            val publishId              = UUID.randomUUID().toString
            val channel                = publisher
            val promise                = Promise[Unit]()
            pending.put(channel.getNextPublishSeqNo, (publishId, promise))
            channel.basicPublish(exchange, routingKey, true, buildProps(event, publishId), event.data.toArray)
            promise.future.map(_ => PublishResult.ok(Json.obj("exchange" -> exchange, "routing_key" -> routingKey)))
          } catch {
            case NonFatal(e) => Future.successful(PublishResult.failed(e))
          }
        }
      }
    }(using io).flatMap { results =>
      Future.sequence(results.map { result =>
        val timeout = org.apache.pekko.pattern.after(30.seconds, env.otoroshiScheduler)(
          Future.successful(PublishResult(error = Some("the broker did not confirm the message within 30 seconds")))
        )
        Future.firstCompletedOf(Seq(result.recover { case NonFatal(e) => PublishResult.failed(e) }, timeout))
      })
    }

  private def toEvent(message: Delivery, queue: String): BrokerMessage = {
    val props                        = Option(message.getProperties)
    val envelope                     = message.getEnvelope
    val headers: Map[String, AnyRef] = props.flatMap(p => Option(p.getHeaders)).map(_.asScala.toMap).getOrElse(Map.empty[String, AnyRef]) - publishIdHeader
    def opt(name: String, value: => Any): Option[(String, String)] = Option(value).map(v => (name, v.toString))
    BrokerMessage(
      key = None,
      data = ByteString(message.getBody),
      headers = headers.collect { case (key, value) if value != null => (key, value.toString) },
      metadata = Map(
        "provider"     -> "rabbitmq",
        "exchange"     -> envelope.getExchange,
        "routing_key"  -> envelope.getRoutingKey,
        "delivery_tag" -> envelope.getDeliveryTag.toString,
        "redelivered"  -> envelope.isRedeliver.toString,
        "queue"        -> queue
      ) ++ headers.get("x-stream-offset").map(offset => "stream_offset" -> offset.toString) ++ props.toSeq.flatMap { p =>
        Seq(
          opt("content_type", p.getContentType),
          opt("content_encoding", p.getContentEncoding),
          opt("delivery_mode", p.getDeliveryMode),
          opt("priority", p.getPriority),
          opt("correlation_id", p.getCorrelationId),
          opt("reply_to", p.getReplyTo),
          opt("expiration", p.getExpiration),
          opt("message_id", p.getMessageId),
          opt("timestamp", Option(p.getTimestamp).map(_.getTime).orNull),
          opt("type", p.getType),
          opt("user_id", p.getUserId),
          opt("app_id", p.getAppId),
          opt("cluster_id", p.getClusterId)
        ).flatten
      }
    )
  }

  /**
   * a queue: its consumers share its messages (that is what a queue is). an exchange and a routing
   * key: a queue of its own, bound to them, gone with the subscription (every subscriber gets every
   * message)
   */
  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = Future {
    val exchange   = spec.options.select("exchange").asOpt[String].filter(_.nonEmpty)
    val routingKey = spec.options.select("routing_key").asOpt[String]
    val prefetch   = spec.options.select("prefetch").asOpt[String].map(_.toInt).getOrElse(100)
    val channel    = connection.createChannel()
    try {
      channel.basicQos(prefetch)
      val queue = exchange match {
        case Some(name) =>
          val own = channel.queueDeclare(s"otoroshi-${UUID.randomUUID()}", false, true, true, null).getQueue
          channel.queueBind(own, name, routingKey.getOrElse(""))
          own
        case None       =>
          val name = spec.topics.headOption.orElse(settings.defaultQueue).getOrElse(throw noQueue)
          // fails, and closes the channel, when there is no such queue
          channel.queueDeclarePassive(name)
          name
      }
      val stream               = settings.streams.contains(queue) || spec.options.select("stream").asOpt[String].contains("true")
      // a stream starts where it is told, from its next message without
      val offset: Option[AnyRef] = spec.from match {
        case StartFrom.Latest               => None
        case _ if !stream                   =>
          throw new IllegalArgumentException(s"only a rabbitmq stream replays: '$queue' is not one (declare it as a stream in the topology, or say stream=true)")
        case StartFrom.Earliest             => Some("first")
        case StartFrom.Timestamp(millis)    => Some(new Date(millis))
        case StartFrom.Offset(offset)       => Some(java.lang.Long.valueOf(offset))
        case StartFrom.Positions(positions, otherwise) =>
          Some(positions.get(queue).flatMap(_.toLongOption).map(last => java.lang.Long.valueOf(last + 1)).getOrElse(if (otherwise.fromEarliest) "first" else "next"))
      }
      given Materializer = env.otoroshiMaterializer
      // never more than `prefetch` unacked messages are delivered: the buffer does not overflow
      val (deliveries, source) = Source.queue[Delivery](prefetch + 16).preMaterialize()
      val tag                  = channel.basicConsume(
        queue,
        false,
        offset.map(o => Map[String, AnyRef]("x-stream-offset" -> o).asJava).orNull,
        new DeliverCallback { override def handle(tag: String, message: Delivery): Unit = { deliveries.offer(message); () } },
        new CancelCallback { override def handle(tag: String): Unit = deliveries.complete() },
        new ConsumerShutdownSignalCallback {
          override def handleShutdownSignal(tag: String, sig: ShutdownSignalException): Unit =
            if (sig.isInitiatedByApplication) deliveries.complete() else deliveries.fail(sig)
        }
      )
      // the acks, then the close: an ack the close overtakes gives its message back to the queue
      val sequence             = new Sequencer(io)
      def ack(message: Delivery): Future[Unit] = sequence(channel.basicAck(message.getEnvelope.getDeliveryTag, false))
      val events               = spec.delivery match {
        // acked as they are handed over: what the buffer holds is still the broker's
        case DeliveryMode.AtMostOnce  => source.map { message => ack(message); ReceivedEvent.unacked(toEvent(message, queue)) }
        case DeliveryMode.AtLeastOnce => source.map(message => new ReceivedEvent(toEvent(message, queue), () => ack(message)))
      }
      // closing the channel gives back what was not acked
      Subscribed(events) { () =>
        sequence {
          Try(channel.basicCancel(tag))
          Try(if (channel.isOpen) channel.close())
        }
        ()
      }
    } catch {
      case NonFatal(e) =>
        Try(if (channel.isOpen) channel.close())
        throw e
    }
  }(using io)
}
