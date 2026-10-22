package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{ClientLease, CloudEvent, CloudEventBindings, PublishResult, Unavailability}
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{EventBrokerPluginConfig, Ingest, Outgoing, Output, OutputFormat, PluginUtils, withStreamOptions}
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.http.websocket.Message
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
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

  /**
   * a frame to publish: a text frame holding a structured cloudevent is that event, anything else is
   * the data of an event (its content type from the `content-type` option, or guessed from the frame)
   */
  private def incoming(message: Message, data: ByteString, contentType: Option[String], defaults: CloudEvent): Outgoing = {
    val isText = message.isInstanceOf[play.api.http.websocket.TextMessage]
    contentType.map(CloudEvent.mediaType) match {
      case Some(Ingest.LegacyContentType) =>
        Try(Json.parse(data.toArray)).toOption.map(json => CloudEvent.fromLegacy(json, defaults)).map(Outgoing(_, _)).getOrElse(Outgoing(defaults.copy(data = Some(data))))
      case _                              =>
        val structured = if (isText && data.headOption.contains('{'.toByte)) Try(Json.parse(data.toArray)).toOption.flatMap(j => CloudEvent.read(j).toOption) else None
        structured.map(Outgoing(_)).getOrElse {
          val guessed = if (!isText) "application/octet-stream" else if (Try(Json.parse(data.toArray)).isSuccess) "application/json" else "text/plain"
          Outgoing(defaults.copy(datacontenttype = Some(contentType.getOrElse(guessed)), data = Some(data)))
        }
    }
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
            def releaseAll(): Unit = {
              leaseIn.foreach(_.release())
              leaseOut.foreach(_.release())
            }
            val outSource: Future[Either[NgProxyEngineError, Source[Message, ?]]] = leaseOut match {
              case None => Future.successful(Right(Source.empty))
              case Some(lease) =>
                OutputFormat.from(optsOut) match {
                  case Left(error) => Future.successful(Left(PluginUtils.badRequest(error)))
                  case Right(format) =>
                    val kind = lease.client.config.kind
                    PluginUtils.subscribe(lease.client, optsOut).map(_.map { events =>
                      lease.releasing(PluginUtils.handOver(events)(r => Output.websocket(CloudEventBindings.fromMessage(r.message, kind), format)).withStreamOptions(optsOut))
                    })
                }
            }
            outSource.map {
              case Left(error) =>
                releaseAll()
                Left(error)
              case Right(out) =>
                given Materializer = env.otoroshiMaterializer
                // one message at a time: a client sending faster than the broker takes waits for it
                val in = Flow[Message]
                  .mapAsync(1)(message => WebsocketMessage.PlayMessage(message).bytes().map(data => (message, data)))
                  .map((message, data) => incoming(message, data, contentTypeOpt, Ingest.defaults(ctx.route, optsIn)))
                  .mapAsync(1) { outgoing =>
                    leaseIn match {
                      case None => Future.successful(Seq.empty)
                      case Some(lease) =>
                        val message = CloudEventBindings.toMessage(outgoing.event, lease.client.config.kind, outgoing.routing)
                        lease.client.publish(Seq(message), optsIn).recover { case NonFatal(e) => Seq(PublishResult.failed(e)) }
                    }
                  }
                  .toMat(Sink.ignore)(Keep.right)
                  .mapMaterializedValue { done =>
                    done.onComplete(_ => leaseIn.foreach(_.release()))
                    done
                  }
                Right(Flow.fromSinkAndSource[Message, Message](in, out))
            }
          }
        }
      }
    }
  }
}
