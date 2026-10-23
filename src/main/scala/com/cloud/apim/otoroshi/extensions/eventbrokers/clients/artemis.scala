package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{ArtemisSettings, EventBrokerConnection}
import org.apache.activemq.artemis.api.core.client.*
import org.apache.activemq.artemis.api.core.{Message, QueueConfiguration, RoutingType, SimpleString}
import org.apache.activemq.artemis.reader.TextMessageUtil
import org.apache.activemq.artemis.utils.UUIDGenerator
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import play.api.libs.json.{JsObject, Json}

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/**
 * ActiveMQ Artemis, through its core client. Artemis routes what is sent to an address:
 *  - multicast: to every queue bound to it. a subscription without a group gets a temporary queue of
 *    its own (every message); with a group, the members share a durable queue named after the group
 *    (what a jms shared durable subscription is), which keeps what is published while none is there
 *  - anycast: to its queue, named after the address, which its consumers share
 *
 * `address::queue` (a fully qualified queue name) reads, or sends to, that queue. Each subscription has
 * a session of its own, closed with it: what was delivered and not acked goes back to its queue.
 * Messages are sent one at a time, each send waiting for the broker, so that every result says what
 * the broker did with it.
 */
class ArtemisBrokerClient(val config: EventBrokerConnection, settings: ArtemisSettings) extends EventBrokerClient with BlockingIo {

  private val logger = play.api.Logger("cloud-apim-event-brokers-artemis")

  private val locatorRef = new AtomicReference[Option[ServerLocator]](None)
  private val factoryRef = new AtomicReference[Option[ClientSessionFactory]](None)

  /** the sessions publishing (one batch at a time each: a session is not to be shared between threads) */
  private val publishers = new ConcurrentLinkedQueue[(ClientSession, ClientProducer)]()
  private val KeptPublishers = 8

  private def factory: ClientSessionFactory = factoryRef.get().getOrElse(throw new IllegalStateException("the artemis client is not started"))

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = true, groups = true, replay = false, keys = true, headers = true)

  private def newSession(): ClientSession =
    // sends and acks committed as they go, acks sent right away
    factory.createSession(settings.username.orNull, settings.password.orNull, false, true, true, false, 0)

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = {
    rememberIo(ec)
    Future {
      val locator = ActiveMQClient
        .createServerLocator(settings.url)
        // a send, a query or an ack waits for the broker, no more than the call timeout, even while reconnecting
        .setBlockOnDurableSend(true)
        .setBlockOnNonDurableSend(true)
        .setCallTimeout(settings.callTimeoutMs)
        .setCallFailoverTimeout(settings.callTimeoutMs)
        .setConsumerWindowSize(settings.consumerWindowSize)
        // the first connection is the manager's to retry, with its own backoff
        .setInitialConnectAttempts(1)
        .setReconnectAttempts(settings.reconnectAttempts)
        .setRetryInterval(1000L)
        .setRetryIntervalMultiplier(2.0)
        .setMaxRetryInterval(30000L)
        // a subscription behind blocks a thread of its own, never one of a shared pool
        .setUseGlobalPools(false)
        .setThreadPoolMaxSize(-1)
      locatorRef.set(Some(locator))
      factoryRef.set(Some(locator.createSessionFactory()))
      // a session of its own says whether the credentials are good
      giveBack(borrow())
    }(using io)
  }

  private def borrow(): (ClientSession, ClientProducer) =
    Option(publishers.poll()).filterNot(_._1.isClosed).getOrElse {
      val session = newSession()
      (session, session.createProducer())
    }

  private def giveBack(publisher: (ClientSession, ClientProducer)): Unit =
    if (publisher._1.isClosed) ()
    else if (publishers.size() < KeptPublishers) publishers.offer(publisher)
    else Try(publisher._1.close())

  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val publisher = borrow()
    try {
      // a round trip to the broker
      publisher._1.addressQuery(SimpleString.of(settings.defaultAddress.getOrElse("otoroshi.healthcheck")))
      giveBack(publisher)
    } catch {
      case NonFatal(e) =>
        Try(publisher._1.close())
        throw e
    }
  }(using io)

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    Iterator.continually(publishers.poll()).takeWhile(_ != null).foreach(p => Try(p._1.close()))
    factoryRef.getAndSet(None).foreach(f => Try(f.close()))
    locatorRef.getAndSet(None).foreach(l => Try(l.close()))
  }(using io)

  private def noAddress = new IllegalArgumentException("no address: none in the call, and the connection has no default_address")

  private val AddressKeys = Seq("address", "topic", "queue")

  /** what the call asks for, or what its name says (`queue`: anycast), or the connection's default */
  private def routingType(options: Map[String, String]): Either[String, RoutingType] =
    options.get("routing_type").map(_.trim.toLowerCase) match {
      case Some("multicast") => Right(RoutingType.MULTICAST)
      case Some("anycast")   => Right(RoutingType.ANYCAST)
      case Some(other)       => Left(s"routing_type is multicast or anycast, not '$other'")
      case None if options.contains("queue") => Right(RoutingType.ANYCAST)
      case None              => Right(if (settings.defaultRoutingType == "anycast") RoutingType.ANYCAST else RoutingType.MULTICAST)
    }

  private def strings(opts: JsObject): Map[String, String] =
    opts.value.collect {
      case (key, value) if value.asOpt[String].isDefined => key -> value.as[String]
      case (key, value) if value.asOpt[Long].isDefined   => key -> value.toString
      case (key, value) if value.asOpt[Boolean].isDefined => key -> value.toString
    }.toMap

  private def longOption(options: Map[String, String], key: String): Option[Long] =
    options.get(key).map(v => v.trim.toLongOption.getOrElse(throw new IllegalArgumentException(s"$key is a number, not '$v'")))

  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] = Future {
    val callOptions = strings(opts)
    val publisher   = borrow()
    try {
      val results = events.map { event =>
        try {
          // what the event says wins over the call
          val options = callOptions ++ event.metadata
          val address = AddressKeys.flatMap(event.metadata.get).headOption
            .orElse(AddressKeys.flatMap(callOptions.get).headOption)
            .orElse(settings.defaultAddress)
            .getOrElse(throw noAddress)
          val routing = routingType(options).fold(e => throw new IllegalArgumentException(e), identity)
          val message = publisher._1.createMessage(Message.BYTES_TYPE, options.get("durable").map(_ == "true").getOrElse(settings.durable))
          message.getBodyBuffer.writeBytes(event.data.toArray)
          event.headers.foreach((name, value) => message.putStringProperty(name, value))
          message.setRoutingType(routing)
          // what jms calls the message id
          val userId  = UUIDGenerator.getInstance().generateUUID()
          message.setUserID(userId)
          // the messages of a group go to one consumer at a time, in order
          event.key.orElse(callOptions.get("key").map(ByteString(_))).foreach(key => message.setGroupID(key.utf8String))
          if (settings.duplicateDetection)
            event.headers.get("cloudEvents_id").foreach { id =>
              message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, SimpleString.of(s"${event.headers.getOrElse("cloudEvents_source", "")}#$id"))
            }
          longOption(options, "priority").foreach { p =>
            if (p < 0 || p > 9) throw new IllegalArgumentException(s"priority is between 0 and 9, not $p")
            message.setPriority(p.toByte)
          }
          val now     = System.currentTimeMillis()
          longOption(options, "ttl").foreach(ttl => message.setExpiration(now + ttl))
          longOption(options, "deliver_at").orElse(longOption(options, "delay").map(now + _)).foreach(at => message.setScheduledDeliveryTime(at))
          publisher._2.send(SimpleString.of(address), message)
          PublishResult.ok(Json.obj("address" -> address, "routing_type" -> routing.toString.toLowerCase, "message_id" -> userId.toString))
        } catch {
          case NonFatal(e) => PublishResult.failed(e)
        }
      }
      giveBack(publisher)
      results
    } catch {
      case NonFatal(e) =>
        Try(publisher._1.close())
        throw e
    }
  }(using io)

  private def isInternal(name: String): Boolean = name.startsWith("_AMQ") || name.startsWith("__AMQ") || name.startsWith("JMSX")

  private def toEvent(message: ClientMessage, queue: String): BrokerMessage = {
    val data       =
      if (message.getType == Message.TEXT_TYPE) Option(TextMessageUtil.readBodyText(message.getBodyBuffer)).map(text => ByteString(text.toString)).getOrElse(ByteString.empty)
      else {
        val bytes = new Array[Byte](message.getBodySize)
        message.getBodyBuffer.readBytes(bytes)
        ByteString(bytes)
      }
    val properties = message.getPropertyNames.asScala.toSeq.map(_.toString).filterNot(isInternal)
    BrokerMessage(
      key = Option(message.getGroupID).map(id => ByteString(id.toString)),
      data = data,
      headers = properties.flatMap(name => Option(message.getObjectProperty(name)).map(value => name -> value.toString)).toMap,
      metadata = Map(
        "provider"       -> "artemis",
        "address"        -> Option(message.getAddress).getOrElse(""),
        "queue"          -> queue,
        "message_id"     -> Option(message.getUserID).map(_.toString).getOrElse(message.getMessageID.toString),
        "delivery_count" -> message.getDeliveryCount.toString,
        "redelivered"    -> (message.getDeliveryCount > 1).toString,
        "priority"       -> message.getPriority.toString,
        "timestamp"      -> message.getTimestamp.toString,
        "durable"        -> message.isDurable.toString
      ) ++ Option(message.getRoutingType).map(r => "routing_type" -> r.toString.toLowerCase) ++
        Option(message.getGroupID).map(id => "group_id" -> id.toString) ++
        Some(message.getExpiration).filter(_ > 0).map(e => "expiration" -> e.toString)
    )
  }

  /** the queue a subscription reads on an address, made if it has to be and the broker lets it */
  private def queueFor(session: ClientSession, address: String, routing: RoutingType, spec: SubscriptionSpec): String = {
    val name = SimpleString.of(address)
    if (address.contains("::")) address
    else
      (routing, spec.group) match {
        case (RoutingType.ANYCAST, Some(_))   =>
          throw new IllegalArgumentException("the consumers of an anycast queue already share it: a group is for a multicast address")
        case (RoutingType.ANYCAST, None)      =>
          val query = session.queueQuery(name)
          if (!query.isExists) {
            if (!query.isAutoCreateQueues) throw new RuntimeException(s"there is no queue '$address', and the broker does not create them")
            session.createQueue(QueueConfiguration.of(name).setAddress(name).setRoutingType(RoutingType.ANYCAST).setDurable(true).setAutoCreated(true))
          }
          address
        case (_, group)                       =>
          if (spec.delivery == DeliveryMode.AtLeastOnce && group.isEmpty)
            throw new IllegalArgumentException("the queue of a multicast subscription without a group goes with it: at-least-once delivery needs a group")
          val existing = session.addressQuery(name)
          if (!existing.isExists) {
            if (!existing.isAutoCreateAddresses) throw new RuntimeException(s"there is no address '$address', and the broker does not create them")
            session.createAddress(name, RoutingType.MULTICAST, true)
          }
          group match {
            case None        =>
              val own = s"otoroshi.${UUID.randomUUID()}"
              session.createQueue(QueueConfiguration.of(own).setAddress(name).setRoutingType(RoutingType.MULTICAST).setDurable(false).setTemporary(true))
              own
            case Some(group) =>
              val query = session.queueQuery(SimpleString.of(group))
              if (!query.isExists)
                Try(session.createQueue(QueueConfiguration.of(group).setAddress(name).setRoutingType(RoutingType.MULTICAST).setDurable(true))).recover {
                  // another member made it in the meantime
                  case NonFatal(_) if session.queueQuery(SimpleString.of(group)).isExists => ()
                }.get
              else if (Option(query.getAddress).map(_.toString).exists(_ != address))
                throw new IllegalArgumentException(s"the queue of the group '$group' is bound to the address '${query.getAddress}', not to '$address'")
              group
          }
      }
  }

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = Future {
    val options   = strings(spec.options)
    val addresses = if (spec.topics.isEmpty) settings.defaultAddress.toSeq else spec.topics.flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty).distinct
    if (addresses.isEmpty) throw noAddress
    if (spec.group.isDefined && addresses.size > 1) throw new IllegalArgumentException("the queue of a group is bound to one address: a group reads one address")
    val routing   = routingType(options).fold(e => throw new IllegalArgumentException(e), identity)
    val prefetch  = options.get("prefetch").map(v => v.trim.toIntOption.filter(n => n >= 1 && n <= 10000).getOrElse(throw new IllegalArgumentException(s"prefetch is a number between 1 and 10000, not '$v'"))).getOrElse(100)
    val selector  = options.get("selector").orElse(options.get("filter")).map(_.trim).filter(_.nonEmpty)
    val session   = newSession()
    try {
      given Materializer = env.otoroshiMaterializer
      val (queue, source) = Source.queue[ReceivedEvent](prefetch + 16).preMaterialize()
      // no more than `prefetch` messages wait in the buffer: the session's delivery thread waits for room,
      // and the broker stops sending once the consumer window is full
      val room            = new Semaphore(prefetch)
      val closed          = new AtomicBoolean(false)
      val consumers       = addresses.map { address =>
        val queueName = queueFor(session, address, routing, spec)
        val consumer  = session.createConsumer(SimpleString.of(queueName), selector.map(SimpleString.of).orNull)
        consumer.setMessageHandler(new MessageHandler {
          override def onMessage(message: ClientMessage): Unit = {
            room.acquire()
            // not acked: back to its queue once the session is closed
            if (closed.get()) room.release()
            else { queue.offer(new ReceivedEvent(toEvent(message, queueName), () => Future { message.individualAcknowledge(); () }(using io))); () }
          }
        })
        consumer
      }
      session.start()
      val events          = source.map { received =>
        room.release()
        received
      }
      val delivered       = spec.delivery match {
        // acked as it leaves: what the buffer holds is still the broker's
        case DeliveryMode.AtMostOnce  =>
          events.map { received =>
            received.ack().failed.foreach(e => logger.warn(s"could not ack a message: ${ConnectionManager.describe(e)}"))(using ExecutionContext.parasitic)
            ReceivedEvent.unacked(received.message)
          }
        case DeliveryMode.AtLeastOnce => events
      }
      Subscribed(delivered) { () =>
        closed.set(true)
        // a delivery thread waiting for room must not hold the session
        room.release(prefetch + 1)
        Future {
          consumers.foreach(c => Try(c.close()))
          Try(session.close())
        }(using io)
        queue.complete()
      }
    } catch {
      case NonFatal(e) =>
        Try(session.close())
        throw e
    }
  }(using io)
}
