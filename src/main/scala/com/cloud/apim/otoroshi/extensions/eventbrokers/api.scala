package com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import otoroshi.env.Env
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.annotation.unused
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/**
 * A message as a broker carries it. Built from a CloudEvent by its binding (`CloudEventBindings`)
 * before being published, and turned back into one when received.
 *
 * `metadata`: on the way out, where the message goes when the call does not say (topic, partition,
 * exchange, ...); on the way in, what the broker says of it (topic, offset, delivery tag, ...).
 */
final case class BrokerMessage(
    key: Option[ByteString] = None,
    data: ByteString = ByteString.empty,
    /** what the broker carries along the payload: kafka headers, amqp application properties, pulsar properties */
    headers: Map[String, String] = Map.empty,
    metadata: Map[String, String] = Map.empty
)

/**
 * A client of one broker, for one version of one connection. Its lifecycle belongs to the
 * `ConnectionManager`: building it must not touch the network, `start` opens what it needs, and
 * nothing is published or consumed before `start` has succeeded.
 *
 * `start`, `healthcheck` and `close` are called on a thread pool made for blocking calls, so they
 * may block (the manager bounds them with timeouts).
 */
trait EventBrokerClient {
  def config: EventBrokerConnection
  def hash: String = config.hash

  /** opens the connections to the broker, fails when it cannot be reached */
  def start()(using env: Env, ec: ExecutionContext): Future[Unit]

  /** a cheap round trip to the broker, fails when it cannot be reached */
  def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit]

  /** releases everything `start` opened. must not fail, and works whether `start` ran or not */
  def close()(using env: Env, ec: ExecutionContext): Future[Unit]

  def capabilities: BrokerCapabilities

  /**
   * how far behind a group is, where the broker tells it (kafka, redis streams): none elsewhere.
   * `topics`: the ones to look at, all the group reads when empty
   */
  def lag(@unused group: String, @unused topics: Seq[String])(using @unused env: Env, @unused ec: ExecutionContext): Future[Option[JsObject]] =
    Future.successful(None)

  /**
   * publishes the events, in order, and answers once the broker has taken them (or refused them):
   * one result per event, in the same order. a broker refusing one event does not fail the others,
   * only a broken client fails the future. `opts` are the options of the call (topic, key, ...); an
   * event's own vendor metadata wins over them
   */
  def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]]

  /**
   * subscribes as `spec` says. the future fails when the subscription cannot be opened (no topic, a
   * queue that does not exist, a consumer the broker refuses): IllegalArgumentException for what the
   * call got wrong, anything else for what the broker said.
   *
   * the subscription lives as long as the source: once it completes, fails or is cancelled,
   * everything it holds on the broker is released (consumer, channel, connection, subscription), and
   * so it is if the source is never run (see `Subscribed`). with `AtLeastOnce`, each event must be
   * acked once it has been handed over, and the unacked ones are delivered again to a later subscriber
   */
  def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]]
}

/** the sources holding broker resources */
object Subscribed {

  /**
   * releases `release` once the source terminates, or after `timeout` if it is never run: the call
   * that asked for it may be gone before its response streams anything
   */
  def apply[A, M](source: Source[A, M], timeout: FiniteDuration = 30.seconds)(release: () => Unit)(using env: Env): Source[A, M] = {
    val released     = new java.util.concurrent.atomic.AtomicBoolean(false)
    val materialized = new java.util.concurrent.atomic.AtomicBoolean(false)
    def releaseOnce(): Unit = if (released.compareAndSet(false, true)) scala.util.Try(release())
    val timer        = env.otoroshiScheduler.scheduleOnce(timeout)(if (!materialized.get()) releaseOnce())(using env.otoroshiExecutionContext)
    source
      .mapMaterializedValue { mat =>
        materialized.set(true)
        timer.cancel()
        mat
      }
      .watchTermination() { (mat, done) =>
        done.onComplete(_ => releaseOnce())(using ExecutionContext.parasitic)
        mat
      }
  }
}

/**
 * the io pool of the manager, kept by the client from its `start`: for the calls of the broker
 * library that block (a send waiting for metadata, a publish waiting for room), which must never run
 * on otoroshi's own threads
 */
trait BlockingIo {
  @volatile private var ioEc: ExecutionContext = null
  protected def rememberIo(ec: ExecutionContext): Unit = ioEc = ec
  protected def io: ExecutionContext = Option(ioEc).getOrElse(throw new IllegalStateException("the client is not started"))
}

/** what a kind of broker can do, so that a subscription asking for something else is refused */
final case class BrokerCapabilities(
    /** at least once delivery: events are acked by the consumer and delivered again when they are not */
    acks: Boolean,
    /** subscribers sharing a group share the events instead of each getting all of them */
    groups: Boolean,
    /** a subscription can start before now: from the earliest event, an offset, a date */
    replay: Boolean,
    keys: Boolean,
    headers: Boolean
) {
  def json: JsObject = Json.obj("acks" -> acks, "groups" -> groups, "replay" -> replay, "keys" -> keys, "headers" -> headers)

  /** why the spec cannot be served, if it cannot */
  def refuse(spec: SubscriptionSpec, kind: String): Option[String] = {
    if (spec.delivery == DeliveryMode.AtLeastOnce && !acks) Some(s"$kind cannot acknowledge events: at-least-once delivery is not available")
    else if (spec.group.isDefined && !groups) Some(s"$kind has no consumer groups")
    else if (spec.from != StartFrom.Latest && !replay) Some(s"$kind only delivers what is published from now on")
    else None
  }
}

enum DeliveryMode(val name: String) {
  /** acked as soon as received: an event lost on the way (a client going away) is not delivered again */
  case AtMostOnce  extends DeliveryMode("at-most-once")
  /** acked once handed over: an event not acked is delivered again, possibly twice */
  case AtLeastOnce extends DeliveryMode("at-least-once")
}

object DeliveryMode {
  def apply(value: String): Option[DeliveryMode] = values.find(_.name == value.trim.toLowerCase)
}

enum StartFrom {
  /** what is published from now on */
  case Latest
  /** everything the broker still has */
  case Earliest
  /** what was published from that date on */
  case Timestamp(epochMillis: Long)
  /** from that offset, on every partition (or the one asked for) */
  case Offset(offset: Long)
  /**
   * right after these positions, what a client resuming gives back. what a position is depends on the
   * broker: `topic:partition` -> offset on kafka, stream -> entry id on redis streams. what they do not
   * name (a partition the client got nothing from yet) starts where `otherwise` says: where the call
   * asked to start
   */
  case Positions(positions: Map[String, String], otherwise: StartFrom = StartFrom.Latest)

  /** from the beginning, or from a position and the beginning for what it does not name */
  def fromEarliest: Boolean = this match {
    case Earliest                => true
    case Positions(_, otherwise) => otherwise.fromEarliest
    case _                       => false
  }
}

/** what to subscribe to. topics are topics, queues or channels, as the broker calls them */
final case class SubscriptionSpec(
    /** empty: the default one of the connection */
    topics: Seq[String] = Seq.empty,
    /** none: a subscription of its own, which gets every event (and is gone with the source) */
    group: Option[String] = None,
    from: StartFrom = StartFrom.Latest,
    delivery: DeliveryMode = DeliveryMode.AtMostOnce,
    /** what only some brokers understand (partition, routing key, qos, subscription type, ...) */
    options: JsObject = Json.obj()
)

/** an event received from a broker, and how to acknowledge it */
final class ReceivedEvent(val message: BrokerMessage, onAck: () => Future[Unit]) {
  private val acked = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** acknowledges the event to the broker. once: acking again does nothing */
  def ack(): Future[Unit] = if (acked.compareAndSet(false, true)) onAck() else Future.unit
}

object ReceivedEvent {
  /** for brokers with nothing to acknowledge */
  def unacked(message: BrokerMessage): ReceivedEvent = new ReceivedEvent(message, () => Future.unit)
}

final case class PublishResult(error: Option[String] = None, details: Option[JsValue] = None) {
  def published: Boolean = error.isEmpty
  def json: JsObject =
    if (published) Json.obj("published" -> true) ++ details.map(d => Json.obj("details" -> d)).getOrElse(Json.obj())
    else Json.obj("published" -> false, "error" -> "publish_error", "error_description" -> error)
}

object PublishResult {
  def ok(details: JsValue): PublishResult = PublishResult(details = Some(details))
  val done: PublishResult                = PublishResult()
  def failed(e: Throwable): PublishResult = PublishResult(error = Some(ConnectionManager.describe(e)))
}
