package com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import org.apache.pekko.stream.scaladsl.Source
import otoroshi.next.plugins.api.{NgPluginConfig, NgPluginHttpRequest}
import otoroshi.next.proxy.NgProxyEngineError
import play.api.libs.json.{Format, JsError, JsObject, JsResult, JsString, JsSuccess, JsValue, Json}
import play.api.mvc.Results
import otoroshi.utils.syntax.implicits._

import otoroshi.env.Env

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/**
 * The configuration of a plugin on a route.
 *
 *  - `options`: what the route fixes (topic, group, ...). values may hold otoroshi expressions
 *    (`orders-${apikey.metadata.tenant}`), evaluated for each call
 *  - `allowed_overrides`: the options a caller may set or change, with `x-ca-event-broker-opts-*`
 *    query params and headers. `*` allows them all. by default only what shapes the caller's own
 *    response (`take`, `drop`, `duration`, `format`, `heartbeat`): anything else is refused with a 400
 *  - `allowed_topics`: glob patterns (`orders-*`) the topics, queues and channels a caller names must
 *    match, when it may name them. empty: any
 */
case class EventBrokerPluginConfig(
    ref: String,
    options: Map[String, String] = Map.empty,
    allowedOverrides: Seq[String] = EventBrokerPluginConfig.defaultOverrides,
    allowedTopics: Seq[String] = Seq.empty,
    includeHeaders: Boolean = true,
    includeQuery: Boolean = true
) extends NgPluginConfig {
  def json: JsValue = EventBrokerPluginConfig.format.writes(this)
}

object EventBrokerPluginConfig {
  /** what only shapes the response of the caller itself */
  val defaultOverrides: Seq[String] = Seq("take", "drop", "duration", "format", "heartbeat")
  val default                       = EventBrokerPluginConfig("")
  given format: Format[EventBrokerPluginConfig] = new Format[EventBrokerPluginConfig] {
    override def reads(json: JsValue): JsResult[EventBrokerPluginConfig] = Try {
      EventBrokerPluginConfig(
        ref = json.select("ref").asOpt[String].getOrElse(""),
        // `config`, then `local_config`: what the previous versions wrote and read
        options = json.select("options").asOpt[Map[String, String]]
          .orElse(json.select("config").asOpt[Map[String, String]])
          .orElse(json.select("local_config").asOpt[Map[String, String]])
          .getOrElse(Map.empty),
        allowedOverrides = json.select("allowed_overrides").asOpt[Seq[String]].getOrElse(defaultOverrides),
        allowedTopics = json.select("allowed_topics").asOpt[Seq[String]].getOrElse(Seq.empty),
        includeHeaders = json.select("include_headers").asOpt[Boolean].getOrElse(true),
        includeQuery = json.select("include_query").asOpt[Boolean].getOrElse(true),
      )
    } match {
      case Failure(e) => JsError(e.getMessage)
      case Success(e) => JsSuccess(e)
    }
    override def writes(o: EventBrokerPluginConfig): JsValue = Json.obj(
      "ref" -> o.ref,
      "options" -> o.options,
      "allowed_overrides" -> o.allowedOverrides,
      "allowed_topics" -> o.allowedTopics,
      "include_headers" -> o.includeHeaders,
      "include_query" -> o.includeQuery,
    )
  }
  val configFlow: Seq[String] = Seq("ref", "options", "allowed_overrides", "allowed_topics", "include_query", "include_headers")
  val configSchema: Option[JsObject] = Some(Json.obj(
    "options" -> Json.obj("type" -> "object", "props" -> Json.obj(
      "label" -> "Options",
      "help" -> "What the route fixes: topic, group, delivery, ... Values may hold expressions like ${apikey.metadata.tenant}"
    )),
    "allowed_overrides" -> Json.obj("type" -> "array", "props" -> Json.obj(
      "label" -> "Options the caller may set",
      "help" -> "With x-ca-event-broker-opts-* query params and headers. * for all of them"
    )),
    "allowed_topics" -> Json.obj("type" -> "array", "props" -> Json.obj(
      "label" -> "Topics the caller may name",
      "help" -> "Glob patterns like orders-*. Empty: any"
    )),
    "include_query" -> Json.obj("type" -> "bool", "props" -> Json.obj("label" -> "Read the caller's options from the query")),
    "include_headers" -> Json.obj("type" -> "bool", "props" -> Json.obj("label" -> "Read the caller's options from the headers")),
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

/** what a call may ask, under the configuration of its plugin */
object CallOptions {

  /** the options that name what to read or where to write: checked against `allowed_topics` */
  val TopicOptions: Set[String] = Set("topic", "topics", "queue", "channel")

  def allows(config: EventBrokerPluginConfig, option: String): Boolean =
    config.allowedOverrides.contains("*") || config.allowedOverrides.contains(option)

  private def glob(pattern: String): scala.util.matching.Regex =
    ("^" + pattern.split("\\*", -1).map(java.util.regex.Pattern.quote).mkString(".*") + "$").r

  def topicAllowed(config: EventBrokerPluginConfig, topic: String): Boolean =
    config.allowedTopics.isEmpty || config.allowedTopics.exists(p => glob(p).matches(topic))

  /** what is wrong with options a caller (or an event) sets, if anything */
  def check(config: EventBrokerPluginConfig, asked: Map[String, String]): Option[String] = {
    val refused = asked.keys.filterNot(allows(config, _)).toSeq.sorted
    val topics  = asked.collect { case (k, v) if TopicOptions.contains(k) => v.split(",").map(_.trim).filter(_.nonEmpty).toSeq }.flatten
    val denied  = topics.filterNot(topicAllowed(config, _))
    if (refused.nonEmpty) Some(s"the caller cannot set ${refused.map(o => s"'$o'").mkString(", ")}")
    else if (denied.nonEmpty) Some(s"the caller cannot use ${denied.map(t => s"'$t'").mkString(", ")}")
    else None
  }

  /** what the caller asks: the prefixed query params and headers, wherever the plugin reads them */
  def asked(config: EventBrokerPluginConfig, request: NgPluginHttpRequest): Map[String, String] = {
    def strip(name: String): Option[String] =
      Option.when(name.toLowerCase.startsWith(PluginUtils.prefix))(name.substring(PluginUtils.prefix.length).toLowerCase)
    val query   = if (!config.includeQuery) Map.empty else request.uri.query().toMap.toSeq.flatMap((k, v) => strip(k).map(_ -> v)).toMap
    val headers = if (!config.includeHeaders) Map.empty else request.headers.toSeq.flatMap((k, v) => strip(k).map(_ -> v)).toMap
    query ++ headers
  }

  /**
   * the options of the call: what the route fixes (its expressions evaluated against the call), then
   * what the caller may change. the caller's values are never evaluated
   */
  def resolve(config: EventBrokerPluginConfig, request: NgPluginHttpRequest, attrs: otoroshi.utils.TypedMap, onlyAllowed: Boolean = false)(using
      env: Env
  ): Either[String, JsObject] = {
    val fixed = config.options.view.mapValues(v => otoroshi.el.GlobalExpressionLanguage.apply(v, attrs, env)).toMap
    // onlyAllowed: keeps what this configuration allows, when another one may allow the rest
    val asked = CallOptions.asked(config, request).filter((k, _) => !onlyAllowed || allows(config, k))
    check(config, asked).toLeft(JsObject((fixed ++ asked).view.mapValues(JsString(_)).toMap))
  }
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

  /** the options of the call, or a 400 (and the lease given back) when the caller asks for what it may not */
  def withOptions[A](config: EventBrokerPluginConfig, request: NgPluginHttpRequest, attrs: otoroshi.utils.TypedMap, lease: ClientLease)(
      f: JsObject => Future[Either[NgProxyEngineError, A]]
  )(using env: Env): Future[Either[NgProxyEngineError, A]] =
    CallOptions.resolve(config, request, attrs) match {
      case Left(error) =>
        lease.release()
        Future.successful(Left(badRequest(error)))
      case Right(opts) => f(opts)
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

  /**
   * opens the subscription the call asks for, or says why it cannot: 400 for the call, 502 for the
   * broker. `resume`: where a client coming back stopped, wins over what the options say
   */
  def subscribe(client: EventBrokerClient, opts: JsObject, resume: Option[StartFrom] = None)(using
      env: Env,
      ec: ExecutionContext
  ): Future[Either[NgProxyEngineError, Source[ReceivedEvent, ?]]] =
    subscriptionSpec(opts).map(spec => resume.map(from => spec.copy(from = from)).getOrElse(spec)) match {
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
  def handOver[A](source: Source[ReceivedEvent, ?])(serialize: ReceivedEvent => A): Source[A, ?] =
    acked(source.map(received => (received, serialize(received))))

  /** the second of each pair goes out, and the event it comes from is acked as it does */
  def acked[A](source: Source[(ReceivedEvent, A), ?]): Source[A, ?] =
    source.map { (received, serialized) =>
      received.ack().failed.foreach(e => logger.warn(s"could not ack an event: ${ConnectionManager.describe(e)}"))(using ExecutionContext.parasitic)
      serialized
    }

  /**
   * publishes in batches, through the cloudevents binding of the broker: one json result per event,
   * in order, with the id of the event. an invalid event, or a broker failing, gives failed results,
   * not a broken stream
   */
  def publishAll(client: EventBrokerClient, events: Source[Either[String, Outgoing], ?], opts: JsObject)(using
      env: Env,
      ec: ExecutionContext
  ): Source[JsObject, ?] =
    events
      .groupedWithin(100, 10.millis)
      .mapAsync(1) { batch =>
        val valid     = batch.collect { case Right(outgoing) => outgoing }
        val messages  = valid.map(o => CloudEventBindings.toMessage(o.event, client.config.kind, o.routing))
        val published =
          if (messages.isEmpty) Future.successful(Seq.empty[PublishResult])
          else Future.fromTry(Try(client.publish(messages, opts))).flatten.recover { case NonFatal(e) => messages.map(_ => PublishResult.failed(e)) }
        published.map { results =>
          val byEvent = valid.zip(results).iterator
          batch.map {
            case Left(error) => Json.obj("published" -> false, "error" -> "invalid_event", "error_description" -> error)
            case Right(_)    =>
              val (outgoing, result) = byEvent.next()
              result.json ++ Json.obj("id" -> outgoing.event.id)
          }
        }
      }
      .mapConcat(identity)


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
