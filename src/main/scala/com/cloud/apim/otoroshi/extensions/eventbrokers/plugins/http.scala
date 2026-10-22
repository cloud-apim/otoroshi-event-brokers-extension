package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.CloudEventBindings
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{CallOptions, EventBrokerPluginConfig, Ingest, Output, OutputFormat, PluginUtils, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json._
import play.api.mvc.Results

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

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

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(_) if !ctx.request.hasBody =>
        Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> "no body provided")))).vfuture
      case Some(extension) => extension.connections.acquire(config.ref) match {
        case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
        case Right(lease) => PluginUtils.withOptions(config, ctx.request, ctx.attrs, lease) { opts =>
          Ingest.events(ctx.request, ctx.route, opts).map {
            case Left(error) =>
              lease.release()
              Left(PluginUtils.badRequest(error))
            case Right(events) =>
              // where an event says it goes is under the same rules as the options of the caller
              val allowed = events.map(_.flatMap(o => CallOptions.check(config, o.routing).toLeft(o)))
              // one json line per event, in order
              val results = PluginUtils.publishAll(lease.client, allowed, opts).map(result => s"${result.stringify}\n".byteString)
              Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(lease.releasing(results)).as("application/x-ndjson")), None))
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

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(extension) => extension.connections.acquire(config.ref) match {
        case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
        case Right(lease) => PluginUtils.withOptions(config, ctx.request, ctx.attrs, lease) { opts =>
          val kind = lease.client.config.kind
          OutputFormat.from(opts) match {
            case Left(error) =>
              lease.release()
              Left(PluginUtils.badRequest(error)).vfuture
            case Right(format) =>
              PluginUtils.subscribe(lease.client, opts).map {
                case Left(error) =>
                  lease.release()
                  Left(error)
                case Right(events) =>
                  val lines  = PluginUtils.handOver(events)(r => Output.ndjson(CloudEventBindings.fromMessage(r.message, kind), format)).withStreamOptions(opts)
                  // an empty line from time to time, when asked: keeps idle proxies from cutting the stream
                  val source = opts.select("heartbeat").asOpt[String].flatMap(_.toLongOption).filter(_ > 0) match {
                    case Some(every) => lines.keepAlive(every.millis, () => ByteString("\n"))
                    case None        => lines
                  }
                  Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Ok.chunked(lease.releasing(source)).as(Output.contentType(format))), None))
              }
          }
        }
      }
    }
  }
}