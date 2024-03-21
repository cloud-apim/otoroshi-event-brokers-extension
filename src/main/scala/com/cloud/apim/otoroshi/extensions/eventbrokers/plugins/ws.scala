package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, PluginUtils}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.http.websocket.Message
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

case class IngesterConsumerWebsocketConfig(in: EventBrokerPluginConfig, out: EventBrokerPluginConfig) extends NgPluginConfig {
  def json: JsValue = IngesterConsumerWebsocketConfig.format.writes(this)
}

object IngesterConsumerWebsocketConfig {
  val default = IngesterConsumerWebsocketConfig(EventBrokerPluginConfig.default, EventBrokerPluginConfig.default)
  val format = new Format[IngesterConsumerWebsocketConfig] {
    override def reads(json: JsValue): JsResult[IngesterConsumerWebsocketConfig] = Try {
      IngesterConsumerWebsocketConfig(
        in = json.select("in").asOpt(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default),
        out = json.select("out").asOpt(EventBrokerPluginConfig.format).getOrElse(EventBrokerPluginConfig.default),
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

  override def callBackend(ctx: NgWebsocketPluginContext)(implicit env: Env, ec: ExecutionContext): Flow[Message, Message, _] = {
    val config = ctx.cachedConfig(internalName)(IngesterConsumerWebsocketConfig.format).getOrElse(IngesterConsumerWebsocketConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Flow.fromSinkAndSource(Sink.ignore, Source.single(play.api.http.websocket.TextMessage("extension not available")))
      case Some(extension) => {
        val clientIn = Option(config.in.ref).filterNot(_.isBlank).flatMap(v => extension.states.connection(v)).map(_.client(extension))
        val clientOut = Option(config.out.ref).filterNot(_.isBlank).flatMap(v => extension.states.connection(v)).map(_.client(extension))
        val request = NgPluginHttpRequest.fromRequest(ctx.request)
        val optsIn = PluginUtils.buildOpts(request, config.in.config, config.in.includeQuery, config.in.includeHeaders)
        val optsOut = PluginUtils.buildOpts(request, config.out.config, config.out.includeQuery, config.out.includeHeaders)
        Flow.fromSinkAndSource(
          Sink.foreach { message =>
            WebsocketMessage.PlayMessage(message).bytes()(env.otoroshiMaterializer, env.otoroshiExecutionContext).map { data =>
              clientIn.foreach(client => client.publish(data, optsIn))
            }
          },
          clientOut match {
            case None => Source.empty
            case Some(client) => {
              val source = client.consume(optsOut).map(w => w.asWsMessage)
                .applyOnWithOpt(optsOut.select("drop").asOpt[String].map(_.toLong)) { case (source, drop) => source.drop(drop) }
                .applyOnWithOpt(optsOut.select("take").asOpt[String].map(_.toLong)) { case (source, limit) => source.take(limit) }
                .applyOnWithOpt(optsOut.select("duration").asOpt[String].map(_.toInt.millis)) { case (source, duration) => source.takeWithin(duration) }
              source
            }
          }
        )
      }
    }
  }
}
