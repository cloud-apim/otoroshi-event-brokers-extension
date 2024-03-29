package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import akka.stream.Materializer
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, PluginUtils}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json._
import play.api.mvc.Results

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class SseEventConsumer extends NgBackendCall {

  override def name: String = "Cloud APIM - SSE events consumer"
  override def description: Option[String] = "Can consume events using SSE".some
  override def core: Boolean = false
  override def visibility: NgPluginVisibility = NgPluginVisibility.NgUserLand
  override def categories: Seq[NgPluginCategory] = Seq(NgPluginCategory.Custom("Cloud APIM"), NgPluginCategory.Custom("Event Brokers"))

  override def steps: Seq[NgStep] = Seq(NgStep.CallBackend)
  override def defaultConfigObject: Option[NgPluginConfig] = Some(EventBrokerPluginConfig.default)
  override def noJsForm: Boolean = true
  override def configFlow: Seq[String] = EventBrokerPluginConfig.configFlow
  override def configSchema: Option[JsObject] = EventBrokerPluginConfig.configSchema

  override def useDelegates: Boolean = false

  override def start(env: Env): Future[Unit] = {
    env.adminExtensions.extension[EventBrokersExtension].foreach { ext =>
      ext.logger.info("the 'SSE events consumer' plugin is available !")
    }
    ().vfuture
  }

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(implicit env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.InternalServerError(Json.obj("error" -> "internal_server_error", "error_description" -> "extension not available")))).vfuture
      case Some(extension) => extension.states.connection(config.ref) match {
        case None => Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.InternalServerError(Json.obj("error" -> "internal_server_error", "error_description" -> "connection not found")))).vfuture
        case Some(connection) => {
          val client = connection.client(extension)
          val opts = PluginUtils.buildOpts(ctx.request, config.config, config.includeQuery, config.includeHeaders)
          val enriched = opts.select("enriched").asOpt[String].map(_.toBoolean).getOrElse(true)
          val source = client.consume(opts).map(w => w.sseSerialize(enriched))
            .applyOnWithOpt(opts.select("drop").asOpt[String].map(_.toLong)) { case (source, drop) => source.drop(drop) }
            .applyOnWithOpt(opts.select("take").asOpt[String].map(_.toLong)) { case (source, limit) => source.take(limit) }
            .applyOnWithOpt(opts.select("duration").asOpt[String].map(_.toInt.millis)) { case (source, duration) => source.takeWithin(duration) }
          Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(source).as("text/event-stream")), None)).vfuture
        }
      }
    }
  }
}
