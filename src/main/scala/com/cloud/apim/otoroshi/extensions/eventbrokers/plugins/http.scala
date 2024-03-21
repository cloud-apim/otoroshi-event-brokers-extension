package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import akka.stream.Materializer
import akka.stream.scaladsl.Framing
import akka.util.ByteString
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

class HttpEventsIngester extends NgBackendCall {

  override def name: String = "Cloud APIM - HTTP events ingester"
  override def description: Option[String] = "Can ingest events and send them to an event broker".some
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
      ext.logger.info("the 'HTTP events ingester' plugin is available !")
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
          if (ctx.request.hasBody) {
            val opts = PluginUtils.buildOpts(ctx.request, config.config, config.includeQuery, config.includeHeaders)
            val isBodyStreamed = ctx.request.contentType.exists(_.startsWith("application/x-ndjson")) ||
              ctx.request.contentType.exists(_.startsWith("application/x-nd-event")) ||
              opts.select("body-streamed").asOpt[String].contains("true")
            val delimiter = opts.select("delimiter").asOpt[String].getOrElse("\n")
            val client = connection.client(extension)
            if (isBodyStreamed) {
              val source = ctx.request.body
                .via(Framing.delimiter(ByteString(delimiter), 32 * 1024 * 1024))
                .mapAsync(1) { chunk =>
                  client.publish(chunk, opts).map {
                    case Left(err) => Json.obj("error" -> "publish_error", "error_details" -> err)
                    case Right(Some(details)) => Json.obj("published" -> true, "details" -> details)
                    case Right(None) => Json.obj("published" -> true)
                  }
                }
                .map(_.stringify.applyOn(str => s"$str\n").byteString)
              Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(source).as("application/x-ndjson")), None)).vfuture
            } else {
              ctx.request.body.runFold(ByteString.empty)(_ ++ _).flatMap { bodyRaw =>
                client.publish(bodyRaw, opts).map {
                  case Left(err) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.InternalServerError(Json.obj("error" -> "publish_error", "error_details" -> err))), None))
                  case Right(Some(details)) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Created(Json.obj("published" -> true, "details" -> details))), None))
                  case Right(None) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Created(Json.obj("published" -> true))), None))
                }
              }
            }
          } else {
            Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> "no body provided")))).vfuture
          }
        }
      }
    }
  }
}

class HttpEventsConsumer extends NgBackendCall {

  override def name: String = "Cloud APIM - HTTP events consumer"
  override def description: Option[String] = "Can consume events from an event broker".some
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
      ext.logger.info("the 'HTTP events consumer' plugin is available !")
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
          val source = client.consume(opts).map(w => w.data ++ ByteString("\n"))
            .applyOnWithOpt(opts.select("drop").asOpt[String].map(_.toLong)) { case (source, drop) => source.drop(drop) }
            .applyOnWithOpt(opts.select("take").asOpt[String].map(_.toLong)) { case (source, limit) => source.take(limit) }
            .applyOnWithOpt(opts.select("duration").asOpt[String].map(_.toInt.millis)) { case (source, duration) => source.takeWithin(duration) }
          Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(source).as("application/x-nd-event")), None)).vfuture
        }
      }
    }
  }
}