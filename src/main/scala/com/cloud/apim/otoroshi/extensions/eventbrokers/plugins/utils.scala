package com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.scaladsl.Source
import otoroshi.next.plugins.api.{NgPluginConfig, NgPluginHttpRequest}
import play.api.libs.json.{Format, JsError, JsObject, JsResult, JsSuccess, JsValue, Json}
import otoroshi.utils.syntax.implicits._

import scala.concurrent.duration.DurationInt
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
