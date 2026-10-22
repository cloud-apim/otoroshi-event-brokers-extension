package com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import org.apache.pekko.stream.scaladsl.Source
import otoroshi.next.plugins.api.{NgPluginConfig, NgPluginHttpRequest}
import otoroshi.next.proxy.NgProxyEngineError
import play.api.libs.json.{Format, JsError, JsObject, JsResult, JsSuccess, JsValue, Json}
import play.api.mvc.Results
import otoroshi.utils.syntax.implicits._

import otoroshi.env.Env

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


case class EventBrokerPluginConfig(ref: String, config: Map[String, String], includeHeaders: Boolean, includeQuery: Boolean) extends NgPluginConfig {
  def json: JsValue = EventBrokerPluginConfig.format.writes(this)
}

object EventBrokerPluginConfig {
  val default = EventBrokerPluginConfig("", Map.empty, true, true)
  given format: Format[EventBrokerPluginConfig] = new Format[EventBrokerPluginConfig] {
    override def reads(json: JsValue): JsResult[EventBrokerPluginConfig] = Try {
      EventBrokerPluginConfig(
        ref = json.select("ref").asOpt[String].getOrElse(""),
        includeHeaders = json.select("include_headers").asOpt[Boolean].getOrElse(true),
        includeQuery = json.select("include_query").asOpt[Boolean].getOrElse(true),
        config = json.select("local_config").asOpt[Map[String, String]].getOrElse(Map.empty),
      )
    } match {
      case Failure(e) => JsError(e.getMessage)
      case Success(e) => JsSuccess(e)
    }
    override def writes(o: EventBrokerPluginConfig): JsValue = Json.obj(
      "ref" -> o.ref,
      "include_headers" -> o.includeHeaders,
      "include_query" -> o.includeQuery,
      "config" -> o.config,
    )
  }
  val configFlow: Seq[String] = Seq("ref", "include_query", "include_headers", "config")
  val configSchema: Option[JsObject] = Some(Json.obj(
    "include_query" -> Json.obj("type" -> "bool", "props" -> Json.obj("label" -> "Include query")),
    "include_headers" -> Json.obj("type" -> "bool", "props" -> Json.obj("label" -> "Include headers")),
    "config" -> Json.obj("type" -> "object", "props" -> Json.obj("label" -> "Configuration")),
    "ref" -> Json.obj(
      "type" -> "select",
      "label" -> s"Event Broker Connection",
      "props" -> Json.obj(
        "optionsFrom" -> s"/bo/api/proxy/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections",
        "optionsTransformer" -> Json.obj(
          "label" -> "name",
          "value" -> "id",
        ),
      ),
    )
  ))
}

object PluginUtils {

  val prefix = "x-ca-event-broker-opts-"

  def extensionNotAvailable: NgProxyEngineError = NgProxyEngineError.NgResultProxyEngineError(
    Results.InternalServerError(Json.obj("error" -> "internal_server_error", "error_description" -> "extension not available"))
  )

  /** a route pointing to a connection that does not exist is misconfigured; one that is not connected (yet) is not */
  def unavailable(reason: Unavailability): NgProxyEngineError = reason match {
    case Unavailability.UnknownConnection(_) =>
      NgProxyEngineError.NgResultProxyEngineError(
        Results.InternalServerError(Json.obj("error" -> "connection_not_found", "error_description" -> reason.message))
      )
    case notConnected: Unavailability.NotConnected =>
      NgProxyEngineError.NgResultProxyEngineError(
        Results
          .ServiceUnavailable(
            Json.obj("error" -> "connection_unavailable", "error_description" -> reason.message, "state" -> notConnected.status.state.name)
          )
          .withHeaders("Retry-After" -> notConnected.retryAfter.toString)
      )
  }

  /**
   * the stream could not be opened: the call misses what it needs (no topic anywhere), or the broker
   * refused it (a consumer it does not allow, a queue that does not exist, ...)
   */
  def consumeError(e: Throwable): NgProxyEngineError = e match {
    case _: IllegalArgumentException =>
      NgProxyEngineError.NgResultProxyEngineError(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> e.getMessage)))
    case _                           =>
      NgProxyEngineError.NgResultProxyEngineError(
        Results.BadGateway(Json.obj("error" -> "consume_error", "error_description" -> ConnectionManager.describe(e)))
      )
  }

  def badRequest(message: String): NgProxyEngineError =
    NgProxyEngineError.NgResultProxyEngineError(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> message)))

  /**
   * what the call asks to subscribe to:
   *  - `topic`, `topics` (comma separated), `queue`, `channel`: what to read (the connection's default without)
   *  - `group` (or `group_id`, `subscription`): share the events with the other subscribers of the group
   *  - `from` (or `auto_offset_reset`): `latest` or `earliest`; `from_timestamp` (epoch millis); `offset_from`
   *  - `delivery`: `at-most-once` (the default) or `at-least-once`
   * every option is also handed to the client, for what only its broker understands
   */
  def subscriptionSpec(opts: JsObject): Either[String, SubscriptionSpec] = {
    def str(key: String): Option[String] = opts.select(key).asOpt[String].map(_.trim).filter(_.nonEmpty)
    val topics   = (str("topics").toSeq.flatMap(_.split(",")) ++ str("topic") ++ str("queue") ++ str("channel")).map(_.trim).filter(_.nonEmpty).distinct
    val group    = str("group").orElse(str("group_id")).orElse(str("subscription"))
    val delivery = str("delivery") match {
      case None        => Right(DeliveryMode.AtMostOnce)
      case Some(value) => DeliveryMode(value).toRight(s"delivery is at-most-once or at-least-once, not '$value'")
    }
    val from     = str("offset_from")
      .map(v => v.toLongOption.map(StartFrom.Offset(_)).toRight(s"offset_from is a number, not '$v'"))
      .orElse(str("from_timestamp").map(v => v.toLongOption.map(StartFrom.Timestamp(_)).toRight(s"from_timestamp is a date in epoch millis, not '$v'")))
      .orElse(str("from").orElse(str("auto_offset_reset")).map(_.toLowerCase).map {
        case "earliest" => Right(StartFrom.Earliest)
        case "latest"   => Right(StartFrom.Latest)
        case other      => Left(s"from is earliest or latest, not '$other'")
      })
      .getOrElse(Right(StartFrom.Latest))
    for {
      d <- delivery
      f <- from
    } yield SubscriptionSpec(topics = topics, group = group, from = f, delivery = d, options = opts)
  }

  /** opens the subscription the call asks for, or says why it cannot: 400 for the call, 502 for the broker */
  def subscribe(client: EventBrokerClient, opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Either[NgProxyEngineError, Source[ReceivedEvent, ?]]] =
    subscriptionSpec(opts) match {
      case Left(error) => Future.successful(Left(badRequest(error)))
      case Right(spec) =>
        client.capabilities.refuse(spec, client.config.kind.label) match {
          case Some(error) => Future.successful(Left(badRequest(error)))
          case None        =>
            Future
              .fromTry(Try(client.subscribe(spec)))
              .flatten
              .map(source => Right(source): Either[NgProxyEngineError, Source[ReceivedEvent, ?]])
              .recover { case NonFatal(e) => Left(consumeError(e)) }
        }
    }

  private val logger = play.api.Logger("cloud-apim-event-brokers-plugins")

  /**
   * serializes the events, and acks each one as it is handed over to the response: at least once
   * means whatever a client did not get (it went away, the stream was cut) is delivered again
   */
  def handOver[A](source: Source[ReceivedEvent, ?])(serialize: EventWrapper => A): Source[A, ?] =
    source
      .map(received => (received, serialize(received.event)))
      .map { (received, serialized) =>
        received.ack().failed.foreach(e => logger.warn(s"could not ack an event: ${ConnectionManager.describe(e)}"))(using ExecutionContext.parasitic)
        serialized
      }

  /** publishes in batches, one result per event, in order: a broker failing gives failed results, not a broken stream */
  def publishAll(client: EventBrokerClient, events: Source[EventWrapper, ?], opts: JsObject)(using env: Env, ec: ExecutionContext): Source[PublishResult, ?] =
    events
      .groupedWithin(100, 10.millis)
      .mapAsync(1) { batch =>
        Future
          .fromTry(Try(client.publish(batch, opts)))
          .flatten
          .recover { case NonFatal(e) => batch.map(_ => PublishResult.failed(e)) }
      }
      .mapConcat(identity)

  def buildOpts(req: NgPluginHttpRequest, localConfig: Map[String, String], includeQuery: Boolean, includeHeaders: Boolean): JsObject = {
    val query: Map[String, String] = if (!includeQuery) Map.empty else req.uri.query().toMap.collect {
      case (key, value) if key.startsWith(prefix) => (key.replaceFirst(prefix, ""), value)
    }
    val headers: Map[String, String] = if (!includeHeaders) Map.empty else req.headers.collect {
      case (key, value) if key.startsWith(prefix) => (key.replaceFirst(prefix, ""), value)
    }
    val all = (query ++ headers ++ localConfig).view.mapValues(_.json).toMap
    JsObject(all)
  }
}

/**
 * The `drop` / `take` / `duration` triple is applied identically by every consuming plugin
 * (http, sse and websocket), so it lives here once.
 */
extension [Out, Mat](source: Source[Out, Mat]) {
  def withStreamOptions(opts: JsObject): Source[Out, Mat] = {
    source
      .applyOnWithOpt(opts.select("drop").asOpt[String].map(_.toLong))((s, drop) => s.drop(drop))
      .applyOnWithOpt(opts.select("take").asOpt[String].map(_.toLong))((s, limit) => s.take(limit))
      .applyOnWithOpt(opts.select("duration").asOpt[String].map(_.toInt.millis))((s, duration) => s.takeWithin(duration))
  }
}
