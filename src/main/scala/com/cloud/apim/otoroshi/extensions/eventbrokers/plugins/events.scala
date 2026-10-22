package com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnectionKind
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Framing, Source}
import org.apache.pekko.util.ByteString
import otoroshi.next.models.NgRoute
import otoroshi.next.plugins.api.NgPluginHttpRequest
import otoroshi.utils.syntax.implicits.*
import play.api.http.websocket.{BinaryMessage, Message, TextMessage}
import play.api.libs.json.*

import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** an event to publish, and where to when the event says (the per event routing of the legacy format) */
final case class Outgoing(event: CloudEvent, routing: Map[String, String] = Map.empty)

/**
 * What the ingesting plugins read from a request:
 *  - `application/cloudevents+json`: one event, structured
 *  - `application/cloudevents-batch+json`: a json array of structured events
 *  - `application/vnd.cloud-apim-event-broker+json`: the format of the extension before cloudevents
 *  - anything else: one event in binary mode, the body as its data, its attributes in `ce-` headers
 * A chunked body (or `lines=true`) is read line by line, each line one event.
 *
 * What a request does not say: a fresh id, `/otoroshi/routes/<route id>` as source (or the `source`
 * option), `com.cloud-apim.event-brokers.event` as type (or the `type` option), now as time.
 */
object Ingest {

  val LegacyContentType = "application/vnd.cloud-apim-event-broker+json"

  def defaults(route: NgRoute, opts: JsObject): CloudEvent =
    CloudEvent
      .generated(
        source = opts.select("source").asOpt[String].getOrElse(s"/otoroshi/routes/${route.id}"),
        `type` = opts.select("type").asOpt[String].getOrElse(CloudEvent.DefaultType)
      )
      .copy(subject = opts.select("subject").asOpt[String])

  private def contentTypeOf(request: NgPluginHttpRequest): Option[String] =
    request.headers.collectFirst { case (name, value) if name.equalsIgnoreCase("content-type") => value }

  private def legacy(json: JsValue, base: () => CloudEvent): Seq[Outgoing] =
    (json \ "events").asOpt[Seq[JsValue]].getOrElse(Seq(json)).map { e =>
      val (event, routing) = CloudEvent.fromLegacy(e, base())
      Outgoing(event, routing)
    }

  private def line(chunk: ByteString, mediaType: Option[String], request: NgPluginHttpRequest, base: () => CloudEvent): Seq[Either[String, Outgoing]] =
    mediaType match {
      case Some(CloudEvent.ContentType) =>
        Seq(Try(Json.parse(chunk.toArray)).toEither.left.map(_ => "not json").flatMap(CloudEvent.read).map(Outgoing(_)))
      case Some(LegacyContentType)      =>
        Try(Json.parse(chunk.toArray)).toOption.map(json => legacy(json, base).map(Right(_))).getOrElse(Seq(Left("not json")))
      case _                            =>
        // the attributes of the headers for every line, each line an event of its own
        Seq(CloudEvent.fromHttp(request.headers.filterNot(_._1.equalsIgnoreCase("ce-id")), chunk, base()).map(Outgoing(_)))
    }

  /**
   * the events of the request, in order. invalid events are lefts: a body read at once that is not
   * valid at all fails the future instead, it is answered with a 400
   */
  def events(request: NgPluginHttpRequest, route: NgRoute, opts: JsObject)(using
      ec: ExecutionContext,
      mat: Materializer
  ): Future[Either[String, Source[Either[String, Outgoing], ?]]] = {
    val contentType = contentTypeOf(request)
    val mediaType   = contentType.map(CloudEvent.mediaType)
    val base        = () => defaults(route, opts)
    val lines       = request.transferEncoding.contains("chunked") || opts.select("lines").asOpt[String].contains("true")
    if (lines) {
      val delimiter = opts.select("delimiter").asOpt[String].getOrElse("\n")
      Future.successful(
        Right(
          request.body
            .via(Framing.delimiter(ByteString(delimiter), 32 * 1024 * 1024, allowTruncation = true))
            .filter(_.nonEmpty)
            .mapConcat(chunk => line(chunk, mediaType, request, base))
        )
      )
    } else {
      request.body.runFold(ByteString.empty)(_ ++ _).map { body =>
        def parsed: Either[String, JsValue] = Try(Json.parse(body.toArray)).toEither.left.map(_ => "the body is not json")
        val all: Either[String, Seq[Outgoing]] = mediaType match {
          case Some(CloudEvent.ContentType)      => parsed.flatMap(CloudEvent.read).map(e => Seq(Outgoing(e)))
          case Some(CloudEvent.BatchContentType) => parsed.flatMap(CloudEvent.readBatch).map(_.map(Outgoing(_)))
          case Some(LegacyContentType)           => parsed.map(json => legacy(json, base))
          case _                                 => CloudEvent.fromHttp(request.headers, body, base()).map(e => Seq(Outgoing(e)))
        }
        all.map(events => Source(events.toList.map(Right(_))))
      }
    }
  }
}

enum OutputFormat(val name: String) {
  /** the structured cloudevent, as json */
  case CloudEvents extends OutputFormat("cloudevents")
  /** the data alone */
  case Raw         extends OutputFormat("raw")
}

object OutputFormat {
  /** `format`: `cloudevents` (the default) or `raw`. `enriched=false` is `raw` too, as before */
  def from(opts: JsObject): Either[String, OutputFormat] =
    opts.select("format").asOpt[String].map(_.trim.toLowerCase) match {
      case Some("cloudevents" | "cloudevent") => Right(CloudEvents)
      case Some("raw")                        => Right(Raw)
      case Some(other)                        => Left(s"format is cloudevents or raw, not '$other'")
      case None                               => Right(if (opts.select("enriched").asOpt[String].contains("false")) Raw else CloudEvents)
    }
}

/** how the consuming plugins write events */
object Output {

  def ndjson(event: CloudEvent, format: OutputFormat): ByteString = format match {
    case OutputFormat.CloudEvents => ByteString(Json.stringify(event.json)) ++ ByteString("\n")
    case OutputFormat.Raw         => event.data.getOrElse(ByteString.empty) ++ ByteString("\n")
  }

  def contentType(format: OutputFormat): String = format match {
    case OutputFormat.CloudEvents => "application/x-ndjson"
    case OutputFormat.Raw         => "application/octet-stream"
  }

  def websocket(event: CloudEvent, format: OutputFormat): Message = format match {
    case OutputFormat.CloudEvents => TextMessage(Json.stringify(event.json))
    case OutputFormat.Raw         =>
      val data = event.data.getOrElse(ByteString.empty)
      if (CloudEvent.isText(event.datacontenttype) || CloudEvent.isJson(event.datacontenttype)) CloudEvent.utf8(data).map(TextMessage(_)).getOrElse(BinaryMessage(data))
      else BinaryMessage(data)
  }

  private def field(value: String): String = value.replaceAll("[\\r\\n]+", " ")

  /** one server-sent event. every line of the data gets its own `data:` field */
  def sse(event: CloudEvent, format: OutputFormat, id: String): ByteString = {
    val payload = format match {
      case OutputFormat.CloudEvents => Json.stringify(event.json)
      case OutputFormat.Raw         =>
        val data = event.data.getOrElse(ByteString.empty)
        CloudEvent.utf8(data).getOrElse(Base64.getEncoder.encodeToString(data.toArray))
    }
    val data    = payload.split("\r\n|\r|\n", -1).map(line => s"data: $line").mkString("\n")
    ByteString(s"id: ${field(id)}\nevent: ${field(event.`type`)}\n$data\n\n")
  }

  /** a comment: keeps the connection (and the proxies on the way) from going idle */
  val sseHeartbeat: ByteString = ByteString(": keep-alive\n\n")
}

/**
 * The `id` of the server-sent events. On kafka, the positions read so far on every partition, so that a
 * client sending it back in `Last-Event-ID` resumes right after them. Elsewhere, the id of the event.
 */
object SseIds {

  private val prefix = "kafka:"

  def kafka(positions: Map[String, Long]): String =
    prefix + Base64.getUrlEncoder.withoutPadding().encodeToString(Json.stringify(JsObject(positions.view.mapValues(JsNumber(_)).toMap)).getBytes("UTF-8"))

  def readKafka(id: String): Option[Map[String, Long]] =
    Option(id).map(_.trim).filter(_.startsWith(prefix)).flatMap { token =>
      Try(Json.parse(Base64.getUrlDecoder.decode(token.drop(prefix.length))).as[Map[String, Long]]).toOption
    }

  /** the position of a kafka message: `topic:partition` and its offset */
  def kafkaPosition(message: BrokerMessage): Option[(String, Long)] = for {
    topic     <- message.metadata.get("topic")
    partition <- message.metadata.get("partition")
    offset    <- message.metadata.get("offset").flatMap(_.toLongOption)
  } yield (s"$topic:$partition", offset)

  /** where to resume from, when the client says where it stopped and the broker can do it */
  def resume(kind: EventBrokerConnectionKind, lastEventId: Option[String]): Option[StartFrom] = kind match {
    case EventBrokerConnectionKind.Kafka => lastEventId.flatMap(readKafka).filter(_.nonEmpty).map(StartFrom.Positions(_))
    case _                               => None
  }
}
