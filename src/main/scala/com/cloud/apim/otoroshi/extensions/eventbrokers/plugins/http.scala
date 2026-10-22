package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Framing, Source}
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.EventWrapper
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, PluginUtils, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json._
import play.api.mvc.Results

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

  private def bodySource(ctx: NgbBackendCallContext, opts: JsObject)(using env: Env, ec: ExecutionContext): (Source[EventWrapper, ?], String) = {
    given mat: Materializer = env.otoroshiMaterializer
    val contentTypeOpt = ctx.request.contentType
    val isBodyStreamed = ctx.request.transferEncoding.contains("chunked") || opts.select("body-streamed").asOpt[String].contains("true")
    val delimiter = opts.select("delimiter").asOpt[String].getOrElse("\n")
    if (isBodyStreamed) {
      val source = ctx.request.body
        .via(Framing.delimiter(ByteString(delimiter), 32 * 1024 * 1024))
        .map { chunk =>
          contentTypeOpt match {
            case Some("application/vnd.cloud-apim-event-broker+json") => EventWrapper.fromJson(chunk.utf8String.parseJson)
            case Some("application/json") => EventWrapper(None, chunk, "text", "json", Map.empty, Map.empty)
            case Some("text/plain") => EventWrapper(None, chunk, "text", "text", Map.empty, Map.empty)
            case _ => EventWrapper(None, chunk, "text", "binary", Map.empty, Map.empty)
          }
        }
      (source, "application/x-ndjson")
    } else {
      (Source.futureSource(ctx.request.body.runFold(ByteString.empty)(_ ++ _).map { bodyRaw =>
        contentTypeOpt match {
          case Some("application/vnd.cloud-apim-event-broker+json") => {
            val parsed = bodyRaw.utf8String.parseJson
            parsed.select("events").asOpt[Seq[JsObject]] match {
              case None => Source.single(EventWrapper.fromJson(parsed))
              case Some(seq) => Source(seq.map(v => EventWrapper.fromJson(v)).toList)
            }
          }
          case Some("application/json") => Source.single(EventWrapper(None, bodyRaw, "text", "json", Map.empty, Map.empty))
          case Some("text/plain") => Source.single(EventWrapper(None, bodyRaw, "text", "text", Map.empty, Map.empty))
          case _ => Source.single(EventWrapper(None, bodyRaw, "text", "binary", Map.empty, Map.empty))
        }
      }), "application/json")
    }
  }

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(_) if !ctx.request.hasBody =>
        Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> "no body provided")))).vfuture
      case Some(extension) => extension.connections.acquire(config.ref) match {
        case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
        case Right(lease) => {
          val opts = PluginUtils.buildOpts(ctx.request, config.config, config.includeQuery, config.includeHeaders)
          val client = lease.client
          val (inSource, responseContentType) = bodySource(ctx, opts)
          val outSource = inSource.mapAsync(1) { chunk =>
              client.publish(chunk, opts, "fooo").map {
                case Left(err) => Json.obj("error" -> "publish_error", "error_details" -> err)
                case Right(Some(details)) => Json.obj("published" -> true, "details" -> details)
                case Right(None) => Json.obj("published" -> true)
              }
            }
            .map(_.stringify.applyOn(str => s"$str\n").byteString)
          Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(lease.releasing(outSource)).as(responseContentType)), None)).vfuture
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

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(extension) => extension.connections.acquire(config.ref) match {
        case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
        case Right(lease) => {
          val opts = PluginUtils.buildOpts(ctx.request, config.config, config.includeQuery, config.includeHeaders)
          val enriched = opts.select("enriched").asOpt[String].map(_.toBoolean).getOrElse(true)
          Try(lease.client.consume(opts)) match {
            case Failure(e) =>
              lease.release()
              Left(PluginUtils.consumeError(e)).vfuture
            case Success(events) =>
              val source = events.map(w => w.httpSerialize(enriched) ++ ByteString("\n")).withStreamOptions(opts)
              Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(lease.releasing(source)).as("application/x-nd-event")), None)).vfuture
          }
        }
      }
    }
  }
}