package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import com.cloud.apim.otoroshi.extensions.eventbrokers.EventWrapper
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, PluginUtils, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.http.websocket.Message
import play.api.libs.json._
import play.api.mvc.Results

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
      case None => NgProxyEngineError.NgResultProxyEngineError(Results.InternalServerError(Json.obj("error" -> "extension not available"))).leftf
      case Some(extension) => {
        val clientIn = Option(config.in.ref).filterNot(_.isBlank).flatMap(v => extension.states.connection(v)).map(_.client())
        val clientOut = Option(config.out.ref).filterNot(_.isBlank).flatMap(v => extension.states.connection(v)).map(_.client())
        val request = NgPluginHttpRequest.fromRequest(ctx.request)
        val optsIn = PluginUtils.buildOpts(request, config.in.config, config.in.includeQuery, config.in.includeHeaders)
        val optsOut = PluginUtils.buildOpts(request, config.out.config, config.out.includeQuery, config.out.includeHeaders)
        val contentTypeOpt = optsIn.select("content-type").asOpt[String]
        Flow.fromSinkAndSource[Message, Message](
          Sink.foreach { message =>
            WebsocketMessage.PlayMessage(message).bytes()(using env.otoroshiMaterializer, env.otoroshiExecutionContext).map { data =>
              val event = contentTypeOpt match {
                case Some("application/vnd.cloud-apim-event-broker+json") => EventWrapper.fromJson(data.utf8String.parseJson)
                case Some("application/json") => EventWrapper(None, data, "text", "json", Map.empty, Map.empty)
                case Some("text/plain") => EventWrapper(None, data, "text", "text", Map.empty, Map.empty)
                case _ => EventWrapper(None, data, "text", "binary", Map.empty, Map.empty)
              }
              clientIn.foreach(client => client.publish(event, optsIn, contentTypeOpt.getOrElse("application/vnd.cloud-apim-event-broker+json")))
            }
          },
          clientOut match {
            case None => Source.empty
            case Some(client) => {
              val enriched = optsOut.select("enriched").asOpt[String].map(_.toBoolean).getOrElse(true)
              client.consume(optsOut).map(w => w.wsMessage(enriched)).withStreamOptions(optsOut)
            }
          }
        ).rightf
      }
    }
  }
}
