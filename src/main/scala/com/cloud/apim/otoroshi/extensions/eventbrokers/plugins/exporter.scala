package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import com.cloud.apim.otoroshi.extensions.eventbrokers.{Integrations, OtoroshiEvents}
import otoroshi.env.Env
import otoroshi.events.{CustomDataExporter, CustomDataExporterContext, ExportResult}
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.*

import scala.concurrent.{ExecutionContext, Future}

/**
 * A data exporter: the events otoroshi exports (analytics, alerts, audit, ...) published to an event
 * broker connection, each one a cloudevent (`io.otoroshi.<@type>`, the event as data, its route as
 * partition key), or the event alone with `format: raw`.
 */
class EventBrokersDataExporter extends CustomDataExporter {

  override def name: String                = "Cloud APIM - Event brokers exporter"
  override def description: Option[String] = "Exports otoroshi events to an event broker connection, as cloudevents".some
  override def defaultConfig: Option[JsObject] =
    Json.obj("connection" -> "", "options" -> Json.obj("topic" -> "otoroshi-events"), "format" -> "cloudevents").some

  override def accept(event: JsValue, ctx: CustomDataExporterContext)(using env: Env): Boolean = true

  override def project(event: JsValue, ctx: CustomDataExporterContext)(using env: Env): JsValue = event

  override def send(events: Seq[JsValue], ctx: CustomDataExporterContext)(using ec: ExecutionContext, env: Env): Future[ExportResult] = {
    val config     = ctx.config
    val connection = (config \ "connection").asOpt[String].getOrElse("")
    val options    = (config \ "options").asOpt[JsObject].getOrElse(Json.obj())
    val raw        = (config \ "format").asOpt[String].contains("raw")
    val cloudEvents = events.map(OtoroshiEvents.toCloudEvent)
    if (connection.isEmpty) Future.successful(ExportResult.ExportResultFailure("no connection in the config of the exporter"))
    else if (events.isEmpty) Future.successful(ExportResult.ExportResultSuccess)
    else
      // raw: the event alone, as the data of a message without attributes
      Integrations.publish(connection, cloudEvents, options, raw).map {
        case Left(error)    => ExportResult.ExportResultFailure(error)
        case Right(results) =>
          results.find(r => !(r \ "published").asOpt[Boolean].contains(true)) match {
            case None         => ExportResult.ExportResultSuccess
            case Some(failed) => ExportResult.ExportResultFailure((failed \ "error_description").asOpt[String].getOrElse(failed.toString))
          }
      }
  }

  override def startExporter(ctx: CustomDataExporterContext)(using ec: ExecutionContext, env: Env): Future[Unit] = Future.unit

  override def stopExporter(ctx: CustomDataExporterContext)(using ec: ExecutionContext, env: Env): Future[Unit] = Future.unit
}
