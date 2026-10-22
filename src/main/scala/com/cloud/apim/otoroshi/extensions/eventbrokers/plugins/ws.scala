package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{ClientLease, EventWrapper, Unavailability}
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, PluginUtils, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.http.websocket.Message
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class IngesterConsumerWebsocketConfig(in: EventBrokerPluginConfig, out: EventBrokerPluginConfig) extends NgPluginConfig {
  def json: JsValue = IngesterConsumerWebsocketConfig.format.writes(this)
}

object IngesterConsumerWebsocketConfig {
  val default = IngesterConsumerWebsocketConfig(EventBrokerPluginConfig.default, EventBrokerPluginConfig.default)
  given format: Format[IngesterConsumerWebsocketConfig] = new Format[IngesterConsumerWebsocketConfig] {
    override def reads(json: JsValue): JsResult[IngesterConsumerWebsocketConfig] = Try {
      IngesterConsumerWebsocketConfig(
        in = json.select("in").asOpt(using EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default),
        out = json.select("out").asOpt(using EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default),
      )
    } match {
      case Failure(e) => JsError(e.getMessage)
      case Success(e) => JsSuccess(e)
    }
    override def writes(o: IngesterConsumerWebsocketConfig): JsValue = Json.obj(
      "in" -> o.in.json,
      "out" -> o.out.json,
    )
  }
  val configFlow: Seq[String] = Seq("in", "out")
  val configSchema: Option[JsObject] = Some(Json.obj(
    "in" -> Json.obj("label" -> "Input config.", "type" -> "form", "collapsable" -> true, "collapsed" -> false, "visible" -> true, "schema" -> EventBrokerPluginConfig.configSchema.get, "flow" -> EventBrokerPluginConfig.configFlow),
    "out" -> Json.obj("label" -> "Output config.", "type" -> "form", "collapsable" -> true, "collapsed" -> false, "visible" -> true, "schema" -> EventBrokerPluginConfig.configSchema.get, "flow" -> EventBrokerPluginConfig.configFlow),
  ))
}

class IngesterConsumerWebsocket extends NgWebsocketBackendPlugin {

  override def name: String = "Cloud APIM - Websocket Ingester/Consumer"
  override def description: Option[String] = "Can ingest and consumer events to/from an event broker".some
  override def core: Boolean = false
  override def visibility: NgPluginVisibility = NgPluginVisibility.NgUserLand
  override def categories: Seq[NgPluginCategory] = Seq(NgPluginCategory.Custom("Cloud APIM"), NgPluginCategory.Custom("Event Brokers"))
  override def steps: Seq[NgStep] = Seq(NgStep.CallBackend)
  override def defaultConfigObject: Option[NgPluginConfig] = Some(IngesterConsumerWebsocketConfig.default)
  override def noJsForm: Boolean = true
  override def configFlow: Seq[String] = IngesterConsumerWebsocketConfig.configFlow
  override def configSchema: Option[JsObject] = IngesterConsumerWebsocketConfig.configSchema

  override def start(env: Env): Future[Unit] = {
    env.adminExtensions.extension[EventBrokersExtension].foreach { ext =>
      ext.logger.info("the 'Websocket Ingester/Consumer' plugin is available !")
    }
    ().vfuture
  }

  override def callBackendOrError(ctx: NgWebsocketPluginContext)(using env: Env, ec: ExecutionContext): Future[Either[NgProxyEngineError, Flow[Message, Message, ?]]] = {
    val config = ctx.cachedConfig(internalName)(IngesterConsumerWebsocketConfig.format).getOrElse(IngesterConsumerWebsocketConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => PluginUtils.extensionNotAvailable.leftf
      case Some(extension) => {
        // no ref on a side: that side is not used
        def lease(ref: String): Either[Unavailability, Option[ClientLease]] =
          Option(ref).filterNot(_.isBlank) match {
            case None     => Right(None)
            case Some(id) => extension.connections.acquire(id).map(Some(_))
          }
        (lease(config.in.ref), lease(config.out.ref)) match {
          case (Left(reason), out) =>
            out.foreach(_.foreach(_.release()))
            PluginUtils.unavailable(reason).leftf
          case (in, Left(reason)) =>
            in.foreach(_.foreach(_.release()))
            PluginUtils.unavailable(reason).leftf
          case (Right(leaseIn), Right(leaseOut)) => {
            val request = NgPluginHttpRequest.fromRequest(ctx.request)
            val optsIn = PluginUtils.buildOpts(request, config.in.config, config.in.includeQuery, config.in.includeHeaders)
            val optsOut = PluginUtils.buildOpts(request, config.out.config, config.out.includeQuery, config.out.includeHeaders)
            val contentTypeOpt = optsIn.select("content-type").asOpt[String]
            val outSource: Either[Throwable, Source[Message, ?]] = leaseOut match {
              case None => Right(Source.empty)
              case Some(lease) => {
                val enriched = optsOut.select("enriched").asOpt[String].map(_.toBoolean).getOrElse(true)
                Try(lease.client.consume(optsOut)).toEither.map(events => lease.releasing(events.map(w => w.wsMessage(enriched)).withStreamOptions(optsOut)))
              }
            }
            outSource match {
              case Left(e) =>
                leaseIn.foreach(_.release())
                leaseOut.foreach(_.release())
                PluginUtils.consumeError(e).leftf
              case Right(out) =>
                val in = Sink
                  .foreach[Message] { message =>
                    WebsocketMessage.PlayMessage(message).bytes()(using env.otoroshiMaterializer, env.otoroshiExecutionContext).map { data =>
                      val event = contentTypeOpt match {
                        case Some("application/vnd.cloud-apim-event-broker+json") => EventWrapper.fromJson(data.utf8String.parseJson)
                        case Some("application/json") => EventWrapper(None, data, "text", "json", Map.empty, Map.empty)
                        case Some("text/plain") => EventWrapper(None, data, "text", "text", Map.empty, Map.empty)
                        case _ => EventWrapper(None, data, "text", "binary", Map.empty, Map.empty)
                      }
                      leaseIn.foreach(lease => lease.client.publish(event, optsIn, contentTypeOpt.getOrElse("application/vnd.cloud-apim-event-broker+json")))
                    }
                  }
                  .mapMaterializedValue { done =>
                    done.onComplete(_ => leaseIn.foreach(_.release()))
                    done
                  }
                Flow.fromSinkAndSource[Message, Message](in, out).rightf
            }
          }
        }
      }
    }
  }
}
