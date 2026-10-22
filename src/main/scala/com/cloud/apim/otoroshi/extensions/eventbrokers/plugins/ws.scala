package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{ClientLease, CloudEvent, CloudEventBindings, PublishResult, Unavailability}
import com.cloud.apim.otoroshi.extensions.eventbrokers.ReceivedEvent
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{CallOptions, EventBrokerPluginConfig, Ingest, Outgoing, Output, OutputFormat, PluginUtils, withStreamOptions}
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

  private def session(
      ctx: NgWebsocketPluginContext,
      config: IngesterConsumerWebsocketConfig,
      leaseIn: Option[ClientLease],
      leaseOut: Option[ClientLease],
      optsIn: JsObject,
      optsOut: JsObject,
      releaseAll: () => Unit
  )(using env: Env, ec: ExecutionContext): Future[Either[NgProxyEngineError, Flow[Message, Message, ?]]] = {
    given Materializer = env.otoroshiMaterializer
    val contentTypeOpt = optsIn.select("content-type").asOpt[String]
    // acks=client: an event is acked when the client says so, with {"ack": "<id>"}, and no more than
    // max_in_flight events wait for theirs
    val clientAcks     = optsOut.select("acks").asOpt[String].contains("client")
    val inFlight       = new InFlight(optsOut.select("max_in_flight").asOpt[String].flatMap(_.toIntOption).filter(_ > 0).getOrElse(100))
    // publish_results=true: every publish is answered with a frame, the result of PublishResult
    val publishResults = optsIn.select("publish_results").asOpt[String].contains("true")
    val (results, resultsSource) = Source.queue[Message](1024).preMaterialize()
    val events: Future[Either[NgProxyEngineError, Source[Message, ?]]] = leaseOut match {
      // nothing to consume: the socket stays open for what the client publishes
      case None => Future.successful(Right(Source.maybe[Message]))
      case Some(lease) =>
        OutputFormat.from(optsOut) match {
          case Left(error) => Future.successful(Left(PluginUtils.badRequest(error)))
          case Right(format) =>
            val kind = lease.client.config.kind
            PluginUtils.subscribe(lease.client, optsOut).map(_.map { received =>
              val frames =
                if (clientAcks)
                  received.mapAsync(1) { r =>
                    val event = CloudEventBindings.fromMessage(r.message, kind)
                    inFlight.add(event.id, r).map(_ => Output.websocket(event, format))
                  }
                else PluginUtils.handOver(received)(r => Output.websocket(CloudEventBindings.fromMessage(r.message, kind), format))
              lease.releasing(frames.withStreamOptions(optsOut))
            })
        }
    }
    events.map {
      case Left(error) =>
        releaseAll()
        Left(error)
      case Right(out) =>
        // one message at a time: a client sending faster than the broker takes waits for it
        val in = Flow[Message]
          .mapAsync(1)(message => WebsocketMessage.PlayMessage(message).bytes().map(data => (message, data)))
          .mapAsync(1) { (message, data) =>
            InFlight.acks(message, data) match {
              case Some(ids) if clientAcks =>
                ids.foreach(inFlight.ack)
                Future.unit
              case _                       =>
                val outgoing = incoming(message, data, contentTypeOpt, Ingest.defaults(ctx.route, optsIn))
                CallOptions.check(config.in, outgoing.routing) match {
                  case Some(error) => answer(results, publishResults, Json.obj("published" -> false, "error" -> "invalid_event", "error_description" -> error))
                  case None        =>
                    leaseIn match {
                      case None => Future.unit
                      case Some(lease) =>
                        val brokerMessage = CloudEventBindings.toMessage(outgoing.event, lease.client.config.kind, outgoing.routing, lease.client.capabilities.headers)
                        lease.client
                          .publish(Seq(brokerMessage), optsIn)
                          .recover { case NonFatal(e) => Seq(PublishResult.failed(e)) }
                          .flatMap(r => answer(results, publishResults, r.headOption.map(_.json).getOrElse(Json.obj()) ++ Json.obj("id" -> outgoing.event.id)))
                    }
                }
            }
          }
          .toMat(Sink.ignore)(Keep.right)
          .mapMaterializedValue { done =>
            done.onComplete { _ =>
              leaseIn.foreach(_.release())
              results.complete()
            }
            done
          }
        Right(Flow.fromSinkAndSource[Message, Message](in, out.merge(resultsSource, eagerComplete = true)))
    }
  }

  private def answer(results: org.apache.pekko.stream.BoundedSourceQueue[Message], enabled: Boolean, json: JsObject): Future[Unit] = {
    if (enabled) results.offer(play.api.http.websocket.TextMessage(Json.stringify(json)))
    Future.unit
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
            def releaseAll(): Unit = {
              leaseIn.foreach(_.release())
              leaseOut.foreach(_.release())
            }
            // both sides read the same query: an option is refused when no side in use allows it, and each
            // side gets what it allows
            val sides   = leaseIn.map(_ => config.in).toSeq ++ leaseOut.map(_ => config.out).toSeq
            val refused = sides.flatMap(c => CallOptions.asked(c, request).keys).distinct.filterNot(k => sides.exists(c => CallOptions.allows(c, k))).sorted
            def side(lease: Option[ClientLease], c: EventBrokerPluginConfig): Either[String, JsObject] =
              if (lease.isEmpty) Right(Json.obj()) else CallOptions.resolve(c, request, ctx.attrs, onlyAllowed = true)
            if (refused.nonEmpty) {
              releaseAll()
              PluginUtils.badRequest(s"the caller cannot set ${refused.map(o => s"'$o'").mkString(", ")}").leftf
            } else {
              (side(leaseIn, config.in), side(leaseOut, config.out)) match {
                case (Left(error), _) =>
                  releaseAll()
                  PluginUtils.badRequest(error).leftf
                case (_, Left(error)) =>
                  releaseAll()
                  PluginUtils.badRequest(error).leftf
                case (Right(optsIn), Right(optsOut)) => session(ctx, config, leaseIn, leaseOut, optsIn, optsOut, () => releaseAll())
              }
            }
          }
        }
      }
    }
  }
}

/**
 * The events sent to a websocket client that acks them itself: at most `max` wait for their ack, the
 * next one waits for room. Acked events are acked to the broker; the others are left to it (delivered
 * again, at least once).
 */
final class InFlight(max: Int) {

  private val waitingForAck = scala.collection.mutable.LinkedHashMap.empty[String, ReceivedEvent]
  private val waitingForRoom = scala.collection.mutable.Queue.empty[(String, ReceivedEvent, scala.concurrent.Promise[Unit])]

  def add(id: String, event: ReceivedEvent): Future[Unit] = synchronized {
    if (waitingForAck.size < max) {
      waitingForAck.put(id, event)
      Future.unit
    } else {
      val room = scala.concurrent.Promise[Unit]()
      waitingForRoom.enqueue((id, event, room))
      room.future
    }
  }

  def ack(id: String): Unit = {
    val acked = synchronized {
      val event = waitingForAck.remove(id)
      while (waitingForAck.size < max && waitingForRoom.nonEmpty) {
        val (next, nextEvent, room) = waitingForRoom.dequeue()
        waitingForAck.put(next, nextEvent)
        room.trySuccess(())
      }
      event
    }
    acked.foreach(_.ack())
  }
}

object InFlight {
  /** the ids of an ack frame: `{"ack": "<id>"}` or `{"ack": ["<id>", ...]}` */
  def acks(message: Message, data: ByteString): Option[Seq[String]] = message match {
    case _: play.api.http.websocket.TextMessage if data.headOption.contains('{'.toByte) =>
      Try(Json.parse(data.toArray)).toOption.flatMap { json =>
        (json \ "ack").toOption.flatMap {
          case JsString(id)  => Some(Seq(id))
          case JsArray(ids)  => Some(ids.toSeq.flatMap(_.asOpt[String]))
          case _             => None
        }
      }
    case _ => None
  }
}
