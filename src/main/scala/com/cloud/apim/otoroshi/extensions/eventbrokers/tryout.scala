package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnectionKind
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{Outgoing, PluginUtils}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import play.api.libs.json.*
import play.api.mvc.{RequestHeader, Result, Results}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Trying a connection from the admin api: publishing a test event, peeking at what a topic or a queue
 * holds. A peek never acks: on a queue (rabbitmq, an anycast address of artemis) or in a group, what
 * it saw goes back to where it was.
 */
object ConnectionTryout {

  private def bad(message: String): Result = Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> message))

  /** 404 for a connection that does not exist, 503 (and when to try again) for one not connected */
  private def unavailable(why: Unavailability)(using env: Env, ec: ExecutionContext): Future[Result] = why match {
    case Unavailability.UnknownConnection(_) => Future.successful(Results.NotFound(Json.obj("error" -> "not_found", "error_description" -> why.message)))
    case _                                   => PluginUtils.unavailable(why).asResult()
  }

  /** the query string as options, the way a consumer route reads them */
  private def options(request: RequestHeader, without: Set[String] = Set.empty): JsObject =
    JsObject(request.queryString.collect { case (k, v) if v.nonEmpty && !without.contains(k) => k -> JsString(v.head) })

  /**
   * the body: a cloudevent, a batch of them, or anything else as the data of a new one. told apart by
   * what it holds: the admin api of otoroshi may not keep the content type a client sent
   */
  private def events(request: RequestHeader, body: ByteString): Either[String, Seq[CloudEvent]] = {
    val declared = request.contentType.orElse(request.headers.get("Content-Type"))
    val headers  = request.headers.toSimpleMap ++ declared.map("Content-Type" -> _)
    def isEvent(json: JsValue): Boolean = (json \ "specversion").asOpt[String].isDefined
    Try(Json.parse(body.toArray)).toOption match {
      case Some(json: JsObject) if isEvent(json)                                   => CloudEvent.read(json).map(Seq(_))
      case Some(json @ JsArray(items)) if items.nonEmpty && items.forall(isEvent) => CloudEvent.readBatch(json)
      case _                                                                     => CloudEvent.fromHttp(headers, body, CloudEvent.generated("/otoroshi/admin-api")).map(Seq(_))
    }
  }

  def publish(connections: ConnectionManager, id: String, request: RequestHeader, body: Option[Source[ByteString, ?]])(using env: Env, ec: ExecutionContext): Future[Result] = {
    given Materializer = env.otoroshiMaterializer
    body.map(_.runFold(ByteString.empty)(_ ++ _)).getOrElse(Future.successful(ByteString.empty)).flatMap { bytes =>
      events(request, bytes) match {
        case Left(error)   => Future.successful(bad(error))
        case Right(events) =>
          connections.acquire(id) match {
            case Left(why)    => unavailable(why)
            case Right(lease) =>
              lease.releasing(
                PluginUtils
                  .publishAll(lease.client, Source(events.toList).map(e => Right(Outgoing(e))), options(request))
                  .runWith(Sink.seq)
                  .map(results => Results.Ok(Json.obj("results" -> results)))
              )
          }
      }
    }
  }

  def peek(connections: ConnectionManager, id: String, request: RequestHeader)(using env: Env, ec: ExecutionContext): Future[Result] = {
    given Materializer = env.otoroshiMaterializer
    val take   = request.getQueryString("take").flatMap(_.toIntOption).getOrElse(10).max(1).min(1000)
    val within = request.getQueryString("within").flatMap(_.toLongOption).getOrElse(5000L).max(100L).min(60000L).millis
    connections.acquire(id) match {
      case Left(why)    => unavailable(why)
      case Right(lease) =>
        val client  = lease.client
        val asked   = options(request, without = Set("take", "within", "delivery"))
        val anycast = asked.value.contains("queue") || (asked \ "routing_type").asOpt[String].contains("anycast")
        // nothing acked: what is acked as it is read is taken away, what is not goes back
        val keep    = client.capabilities.acks && (asked.value.contains("group") || client.config.kind == EventBrokerConnectionKind.RabbitMQ ||
          (client.config.kind == EventBrokerConnectionKind.Artemis && anycast))
        val opts    = asked ++ Json.obj("delivery" -> (if (keep) "at-least-once" else "at-most-once"))
        lease.releasing(
          PluginUtils.subscribe(client, opts).flatMap {
            case Left(error)   => error.asResult()
            case Right(events) =>
              events
                .take(take.toLong)
                .takeWithin(within)
                .map(received => CloudEventBindings.fromMessage(received.message, client.config.kind).json)
                .runWith(Sink.seq)
                .map(found => Results.Ok(Json.obj("events" -> found, "acked" -> !keep)))
          }.recover { case NonFatal(e) => Results.BadGateway(Json.obj("error" -> "peek_error", "error_description" -> ConnectionManager.describe(e))) }
        )
    }
  }
}
