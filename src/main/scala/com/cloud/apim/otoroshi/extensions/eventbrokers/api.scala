package com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import otoroshi.env.Env
import play.api.libs.json.{JsNull, JsObject, JsValue, Json}
import otoroshi.utils.syntax.implicits._

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

object EventWrapper {
  def fromJson(json: JsValue): EventWrapper = {
    val keyContentType = json.select("key").select("type").asOpt[String].getOrElse("binary")
    val dataContentType = json.select("value").select("type").asOpt[String].getOrElse("binary")
    val key: Option[ByteString] = keyContentType.toLowerCase() match {
      case "text" => json.select("key").select("data").asOpt[String].map(_.byteString)
      case "text/plain" => json.select("key").select("data").asOpt[String].map(_.byteString)
      case "json" => json.select("key").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "application/json" => json.select("key").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "binary" => json.select("key").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case "application/octet-stream" => json.select("key").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case _ => None
    }
    val data: Option[ByteString] = dataContentType.toLowerCase() match {
      case "text" =>                     json.select("value").select("data").asOpt[String].map(_.byteString)
      case "text/plain" =>               json.select("value").select("data").asOpt[String].map(_.byteString)
      case "json" =>                     json.select("value").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "application/json" =>         json.select("value").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "binary" =>                   json.select("value").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case "application/octet-stream" => json.select("value").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case _ => None
    }
    EventWrapper(
      key = key,
      data = data.getOrElse(ByteString.empty),
      keyContentType = keyContentType,
      dataContentType = dataContentType,
      headers = json.select("headers").asOpt[Map[String, String]].getOrElse(Map.empty),
      vendorMetadata = json.select("vendor_metadata").asOpt[Map[String, String]].getOrElse(Map.empty),
    )
  }
}

case class EventWrapper(
  key: Option[ByteString],
  data: ByteString,
  keyContentType: String,
  dataContentType: String,
  headers: Map[String, String],
  vendorMetadata: Map[String, String]
) {
  def wsMessage(enriched: Boolean): play.api.http.websocket.Message = {
    if (enriched) {
      play.api.http.websocket.TextMessage(json.stringify)
    } else {
      play.api.http.websocket.BinaryMessage(data)
    }
  }
  def sseSerialize(enriched: Boolean): ByteString = {
    if (enriched) {
      key match {
        case Some(id) => {
          val key: String = keyContentType.toLowerCase() match {
            case "text" => id.utf8String
            case "text/plain" => id.utf8String
            case "json" => id.utf8String
            case "application/json" => id.utf8String
            case _ => id.encodeBase64.utf8String
          }
          s"id: ${key}\ndata: ${json.stringify}\n\n".byteString
        } // TODO: support event: kind
        case None => s"data: ${json.stringify}\n\n".byteString
      }
    } else {
      key match {
        case Some(id) => {
          val key: String = keyContentType.toLowerCase() match {
            case "text" => id.utf8String
            case "text/plain" => id.utf8String
            case "json" => id.utf8String
            case "application/json" => id.utf8String
            case _ => id.encodeBase64.utf8String
          }
          s"id: ${key}\ndata: ${data.utf8String}\n\n".byteString
        } // TODO: support event: kind
        case None => s"data: ${data.utf8String}\n\n".byteString
      }
    }
  }
  def httpSerialize(enriched: Boolean): ByteString = {
    if (enriched) {
      json.stringify.byteString
    } else {
      data
    }
  }
  def jsonKey: JsValue = {
    key match {
      case None => JsNull
      case Some(key) => {
        val data = keyContentType.toLowerCase() match {
          case "text" => key.utf8String.json
          case "text/plain" => key.utf8String.json
          case "json" => key.utf8String.parseJson
          case "application/json" => key.utf8String.parseJson
          case "binary" => key.encodeBase64.utf8String.json
          case "application/octet-stream" => key.encodeBase64.utf8String.json
          case _ => JsNull
        }
        Json.obj(
          "type" -> keyContentType,
          "data" -> data
        )
      }
    }
  }
  def jsonValue: JsValue = {
    val finaldata = dataContentType.toLowerCase() match {
      case "text" => data.utf8String.json
      case "text/plain" => data.utf8String.json
      case "json" => data.utf8String.parseJson
      case "application/json" => data.utf8String.parseJson
      case "binary" => data.encodeBase64.utf8String.json
      case "application/octet-stream" => data.encodeBase64.utf8String.json
      case _ => JsNull
    }
    Json.obj(
      "type" -> dataContentType,
      "data" -> finaldata
    )
  }
  def json: JsValue = Json.obj(
    "key" -> jsonKey,
    "value" -> jsonValue,
    "headers" -> headers,
    "timestamp" -> headers.get("timestamp").orElse(vendorMetadata.get("timestamp")).map(_.json).getOrElse(JsNull).asValue,
    "vendor_metadata" -> vendorMetadata,
  )
}

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
   * publishes the events, in order, and answers once the broker has taken them (or refused them):
   * one result per event, in the same order. a broker refusing one event does not fail the others,
   * only a broken client fails the future. `opts` are the options of the call (topic, key, ...); an
   * event's own vendor metadata wins over them
   */
  def publish(events: Seq[EventWrapper], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]]

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
final class ReceivedEvent(val event: EventWrapper, onAck: () => Future[Unit]) {
  private val acked = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** acknowledges the event to the broker. once: acking again does nothing */
  def ack(): Future[Unit] = if (acked.compareAndSet(false, true)) onAck() else Future.unit
}

object ReceivedEvent {
  /** for brokers with nothing to acknowledge */
  def unacked(event: EventWrapper): ReceivedEvent = new ReceivedEvent(event, () => Future.unit)
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
