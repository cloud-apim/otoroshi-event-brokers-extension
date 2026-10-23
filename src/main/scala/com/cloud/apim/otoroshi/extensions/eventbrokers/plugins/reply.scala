package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, Ingest, Outgoing, PluginUtils}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{ConnectionManager, ReplyListener}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.next.plugins.api.*
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits.*
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.*
import play.api.mvc.Results

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Request-reply over a broker: the body of the call is published as an event (as the ingester reads
 * it) with the `correlationid` and `replyto` extensions, and the response is the data of the reply,
 * the event published where `replyto` says with the same `correlationid`.
 *
 * Options: `reply_to` (required: where the replies go, given to the service in `replyto`),
 * `reply_options` (json: how to read them, `{"topic": <reply_to>}` by default), `timeout` (millis, 10000).
 * A reply may say its http status in a `status` extension.
 */
class HttpEventsRequestReply extends NgBackendCall {

  override def name: String                      = "Cloud APIM - Events request reply"
  override def description: Option[String]       = "Publishes the request as an event, and answers with the event that replies to it".some
  override def core: Boolean                     = false
  override def visibility: NgPluginVisibility    = NgPluginVisibility.NgUserLand
  override def categories: Seq[NgPluginCategory] = Seq(NgPluginCategory.Custom("Cloud APIM"), NgPluginCategory.Custom("Event Brokers"))

  override def steps: Seq[NgStep]                          = Seq(NgStep.CallBackend)
  override def defaultConfigObject: Option[NgPluginConfig] = Some(EventBrokerPluginConfig.default)
  override def noJsForm: Boolean                           = true
  override def configFlow: Seq[String]                     = EventBrokerPluginConfig.configFlow
  override def configSchema: Option[JsObject]              = EventBrokerPluginConfig.configSchema

  override def useDelegates: Boolean = false

  private def error(status: Int, code: String, message: String): NgProxyEngineError =
    NgProxyEngineError.NgResultProxyEngineError(Results.Status(status)(Json.obj("error" -> code, "error_description" -> message)))

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(using
      env: Env,
      ec: ExecutionContext,
      mat: Materializer
  ): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None            => Left(PluginUtils.extensionNotAvailable).vfuture
      case Some(extension) =>
        extension.connections.acquire(config.ref) match {
          case Left(reason) => Left(PluginUtils.unavailable(reason)).vfuture
          case Right(lease) =>
            PluginUtils.withOptions(config, ctx.request, ctx.attrs, lease) { opts =>
              val replyTo      = opts.select("reply_to").asOpt[String].filter(_.trim.nonEmpty)
              val replyOptions = opts.select("reply_options").asOpt[String].flatMap(s => Try(Json.parse(s).as[JsObject]).toOption)
                .orElse(opts.select("reply_options").asOpt[JsObject])
                .orElse(replyTo.map(r => Json.obj("topic" -> r)))
              val timeout      = opts.select("timeout").asOpt[String].flatMap(_.toLongOption).getOrElse(10000L).millis
              (replyTo, replyOptions) match {
                case (None, _) | (_, None)               =>
                  lease.release()
                  Left(PluginUtils.badRequest("reply_to is required: where the service answering sends its replies")).vfuture
                case (Some(replyTo), Some(replyOptions)) =>
                  val listener = extension.replies.listener(config.ref, replyOptions)
                  Ingest.events(ctx.request, ctx.route, opts).flatMap {
                    case Left(why)     =>
                      lease.release()
                      Left(PluginUtils.badRequest(why)).vfuture
                    case Right(events) =>
                      events.runWith(Sink.headOption).flatMap {
                        case None                    =>
                          lease.release()
                          Left(PluginUtils.badRequest("no event in the request")).vfuture
                        case Some(Left(why))         =>
                          lease.release()
                          Left(PluginUtils.badRequest(why)).vfuture
                        case Some(Right(outgoing))   =>
                          val correlationId = UUID.randomUUID().toString
                          val request       = outgoing.event.copy(extensions = outgoing.event.extensions ++ Map("correlationid" -> JsString(correlationId), "replyto" -> JsString(replyTo)))
                          // the fields of the brokers that have them
                          val routing       = outgoing.routing ++ Map("reply_to" -> replyTo, "correlation_id" -> correlationId, "response_topic" -> replyTo)
                          val reply         = listener.expect(correlationId, timeout)
                          // a broker that replays reads the replies from before they could be read
                          val ready         = if (ReplyListener.fromDate(lease.client)) Future.unit else listener.whenReady(timeout)
                          val published     = ready.flatMap { _ =>
                            lease.releasing(PluginUtils.publishAll(lease.client, Source.single(Right(Outgoing(request, routing))), opts).runWith(Sink.head))
                          }
                          published.flatMap { result =>
                            if (!(result \ "published").asOpt[Boolean].contains(true)) {
                              listener.forget(correlationId)
                              Left(error(502, "publish_error", (result \ "error_description").asOpt[String].getOrElse("the request could not be published"))).vfuture
                            } else
                              reply.map { event =>
                                val status = event.extension("status").flatMap(_.toIntOption).getOrElse(200)
                                val body   = event.data.getOrElse(ByteString.empty)
                                Right(
                                  BackendCallResponse(
                                    NgPluginHttpResponse.fromResult(
                                      Results.Status(status)(body).as(event.datacontenttype.getOrElse("application/octet-stream")).withHeaders("X-Correlation-Id" -> correlationId)
                                    ),
                                    None
                                  )
                                )
                              }
                          }.recover {
                            case e: java.util.concurrent.TimeoutException => Left(error(504, "no_reply", e.getMessage))
                            case NonFatal(e)                              => Left(error(502, "request_reply_error", ConnectionManager.describe(e)))
                          }
                      }
                  }
              }
            }
        }
    }
  }
}
