package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.Materializer
import com.cloud.apim.otoroshi.extensions.eventbrokers.CloudEventBindings
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, Output, OutputFormat, PluginUtils, SseIds, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json._
import play.api.mvc.Results

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

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

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(extension) => extension.connections.acquire(config.ref) match {
        case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
        case Right(lease) => PluginUtils.withOptions(config, ctx.request, ctx.attrs, lease) { opts =>
          val kind = lease.client.config.kind
          // what a client reconnecting got last: a broker that can replay resumes right after it
          val lastEventId = ctx.request.headers
            .collectFirst { case (name, value) if name.equalsIgnoreCase("last-event-id") => value }
            .filter(_ => lease.client.capabilities.replay)
          OutputFormat.from(opts) match {
            case Left(error) =>
              lease.release()
              Left(PluginUtils.badRequest(error)).vfuture
            case Right(format) =>
              PluginUtils.subscribe(lease.client, opts, SseIds.resume(kind, lastEventId), Some(ctx.route.id)).map {
                case Left(error) =>
                  lease.release()
                  Left(error)
                case Right(events) =>
                  val resumed   = lastEventId.flatMap(SseIds.read(kind, _)).getOrElse(Map.empty)
                  val framed    = events.statefulMap(() => resumed)(
                    (positions, received) => {
                      val event = CloudEventBindings.fromMessage(received.message, kind)
                      val next  = SseIds.position(kind, received.message).map(positions + _).getOrElse(positions)
                      val id    = if (next.nonEmpty) SseIds.token(kind, next) else event.id
                      (next, (received, Output.sse(event, format, id)))
                    },
                    _ => None
                  )
                  val sse       = PluginUtils.acked(framed).withStreamOptions(opts)
                  val heartbeat = opts.select("heartbeat").asOpt[String].flatMap(_.toLongOption).getOrElse(15000L)
                  val source    = if (heartbeat > 0) sse.keepAlive(heartbeat.millis, () => Output.sseHeartbeat) else sse
                  Right(
                    BackendCallResponse(
                      NgPluginHttpResponse.fromResult(
                        Results.Ok
                          .chunked(lease.releasing(source))
                          .as("text/event-stream")
                          // no proxy on the way should keep events in a buffer
                          .withHeaders("Cache-Control" -> "no-cache", "X-Accel-Buffering" -> "no")
                      ),
                      None
                    )
                  )
              }
          }
        }
      }
    }
  }
}
