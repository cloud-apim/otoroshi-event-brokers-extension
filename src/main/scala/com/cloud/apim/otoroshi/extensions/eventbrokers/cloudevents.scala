package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnectionKind
import org.apache.pekko.util.ByteString
import play.api.libs.json.*

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.{Base64, UUID}
import scala.util.Try

/**
 * A CloudEvents 1.0 event: what the plugins take in and give out, whatever the broker.
 * https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
 *
 * `data` is kept as bytes; how it is written in json depends on `datacontenttype`.
 */
final case class CloudEvent(
    id: String,
    source: String,
    `type`: String,
    datacontenttype: Option[String] = None,
    dataschema: Option[String] = None,
    subject: Option[String] = None,
    /** rfc 3339 */
    time: Option[String] = None,
    data: Option[ByteString] = None,
    /** strings, numbers or booleans, named with lowercase letters and digits */
    extensions: Map[String, JsValue] = Map.empty
) {

  def extension(name: String): Option[String] = extensions.get(name).map {
    case JsString(s) => s
    case other       => other.toString
  }

  /** the structured json form */
  def json: JsObject = {
    val attributes = Json.obj("specversion" -> CloudEvent.SpecVersion, "id" -> id, "source" -> source, "type" -> `type`) ++
      JsObject(
        Seq("datacontenttype" -> datacontenttype, "dataschema" -> dataschema, "subject" -> subject, "time" -> time).collect {
          case (name, Some(value)) => name -> JsString(value)
        }
      ) ++ JsObject(extensions.toSeq.sortBy(_._1))
    attributes ++ data.map(CloudEvent.writeData(_, datacontenttype)).getOrElse(Json.obj())
  }
}

object CloudEvent {

  val SpecVersion      = "1.0"
  val ContentType      = "application/cloudevents+json"
  val BatchContentType = "application/cloudevents-batch+json"
  /** what an event published without a type is */
  val DefaultType      = "com.cloud-apim.event-brokers.event"
  /** what a message received without cloudevents attributes is */
  val MessageType      = "com.cloud-apim.event-brokers.message"

  val ContextAttributes: Set[String] = Set("specversion", "id", "source", "type", "datacontenttype", "dataschema", "subject", "time")

  def now(): String = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
  def time(epochMillis: Long): String = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(epochMillis))

  def mediaType(contentType: String): String = contentType.split(";").head.trim.toLowerCase

  /** no content type is json, as the json format says */
  def isJson(contentType: Option[String]): Boolean = contentType.forall { ct =>
    val mt = mediaType(ct)
    mt == "application/json" || mt.endsWith("+json")
  }

  def isText(contentType: Option[String]): Boolean = contentType.exists { ct =>
    val mt = mediaType(ct)
    mt.startsWith("text/") || mt == "application/xml" || mt.endsWith("+xml") || mt == "application/x-www-form-urlencoded"
  }

  def isValidName(name: String): Boolean = name.nonEmpty && name.forall(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))

  def utf8(bytes: ByteString): Option[String] = {
    val decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
    try Some(decoder.decode(ByteBuffer.wrap(bytes.toArray)).toString)
    catch { case _: CharacterCodingException => None }
  }

  private def writeData(data: ByteString, contentType: Option[String]): JsObject = {
    def base64 = Json.obj("data_base64" -> Base64.getEncoder.encodeToString(data.toArray))
    if (isJson(contentType)) Try(Json.parse(data.toArray)).toOption.map(v => Json.obj("data" -> v)).getOrElse(base64)
    else if (isText(contentType)) utf8(data).map(s => Json.obj("data" -> s)).getOrElse(base64)
    else base64
  }

  /** an extension value: strings, numbers and booleans only */
  private def extensionValue(value: JsValue): Option[JsValue] = value match {
    case v @ (JsString(_) | JsNumber(_) | JsBoolean(_)) => Some(v)
    case _                                                => None
  }

  /** reads the structured json form */
  def read(json: JsValue): Either[String, CloudEvent] = json match {
    case obj: JsObject =>
      def string(name: String): Option[String] = (obj \ name).asOpt[String].filter(_.nonEmpty)
      val errors          = Seq(
        Option.when(!(obj \ "specversion").asOpt[String].contains(SpecVersion))(s"specversion must be $SpecVersion"),
        Option.when(string("id").isEmpty)("id is required"),
        Option.when(string("source").isEmpty)("source is required"),
        Option.when(string("type").isEmpty)("type is required"),
        Option.when(obj.keys.contains("data") && obj.keys.contains("data_base64"))("data and data_base64 cannot be both there"),
        Option.when((obj \ "data_base64").toOption.exists(v => v.asOpt[String].forall(s => Try(Base64.getDecoder.decode(s)).isFailure)))("data_base64 is not base64")
      ).flatten ++ obj.fields.collect {
        case (name, _) if !ContextAttributes.contains(name) && name != "data" && name != "data_base64" && !isValidName(name) =>
          s"'$name' is not a valid attribute name (lowercase letters and digits)"
        case (name, value) if !ContextAttributes.contains(name) && name != "data" && name != "data_base64" && extensionValue(value).isEmpty =>
          s"the extension '$name' must be a string, a number or a boolean"
      }
      if (errors.nonEmpty) Left(errors.mkString(", "))
      else {
        val contentType = string("datacontenttype")
        val data        = (obj \ "data_base64").asOpt[String].map(s => ByteString(Base64.getDecoder.decode(s))).orElse {
          (obj \ "data").toOption.filter(_ != JsNull).map {
            case JsString(s) if !isJson(contentType) => ByteString(s)
            case other                               => ByteString(Json.stringify(other))
          }
        }
        Right(
          CloudEvent(
            id = string("id").get,
            source = string("source").get,
            `type` = string("type").get,
            datacontenttype = contentType,
            dataschema = string("dataschema"),
            subject = string("subject"),
            time = string("time"),
            data = data,
            extensions = obj.fields.collect {
              case (name, value) if !ContextAttributes.contains(name) && name != "data" && name != "data_base64" => name -> value
            }.toMap
          )
        )
      }
    case _             => Left("a cloudevent is a json object")
  }

  def readBatch(json: JsValue): Either[String, Seq[CloudEvent]] = json match {
    case JsArray(values) =>
      val read = values.toSeq.zipWithIndex.map((v, i) => CloudEvent.read(v).left.map(e => s"event $i: $e"))
      read.collectFirst { case Left(e) => e }.toLeft(read.collect { case Right(e) => e })
    case _               => Left("a batch of cloudevents is a json array")
  }

  /**
   * the binary mode of the http binding: the attributes in `ce-` headers, the data in the body, its
   * content type in `content-type`. what is missing comes from `defaults`
   */
  def fromHttp(headers: Map[String, String], body: ByteString, defaults: CloudEvent): Either[String, CloudEvent] = {
    val ce          = headers.collect { case (name, value) if name.toLowerCase.startsWith("ce-") => name.toLowerCase.drop(3) -> value }
    val invalid     = ce.keys.filterNot(n => ContextAttributes.contains(n) || isValidName(n))
    if (invalid.nonEmpty) Left(s"invalid attribute names: ${invalid.mkString(", ")}")
    else if (ce.get("specversion").exists(_ != SpecVersion)) Left(s"ce-specversion must be $SpecVersion")
    else
      Right(
        defaults.copy(
          id = ce.getOrElse("id", defaults.id),
          source = ce.getOrElse("source", defaults.source),
          `type` = ce.getOrElse("type", defaults.`type`),
          datacontenttype = headers.collectFirst { case (n, v) if n.equalsIgnoreCase("content-type") => v }.orElse(defaults.datacontenttype),
          dataschema = ce.get("dataschema").orElse(defaults.dataschema),
          subject = ce.get("subject").orElse(defaults.subject),
          time = ce.get("time").orElse(defaults.time),
          data = Some(body),
          extensions = defaults.extensions ++ ce.filterNot((n, _) => ContextAttributes.contains(n)).view.mapValues(JsString(_)).toMap
        )
      )
  }

  def generated(source: String, `type`: String = DefaultType): CloudEvent =
    CloudEvent(id = UUID.randomUUID().toString, source = source, `type` = `type`, time = Some(now()))

  /**
   * the format of the extension before cloudevents: `{"key": {"type", "data"}, "value": {"type",
   * "data"}, "headers": {}, "vendor_metadata": {}}`, still read. `vendor_metadata` routes the event
   */
  def fromLegacy(json: JsValue, defaults: CloudEvent): (CloudEvent, Map[String, String]) = {
    def bytes(part: JsLookupResult): Option[(ByteString, String)] = {
      val tpe = (part \ "type").asOpt[String].getOrElse("binary").toLowerCase
      (part \ "data").toOption.filter(_ != JsNull).map { data =>
        tpe match {
          case "text" | "text/plain"       => (ByteString(data.asOpt[String].getOrElse(data.toString)), "text/plain")
          case "json" | "application/json" => (ByteString(Json.stringify(data)), "application/json")
          case _                           => (ByteString(Base64.getDecoder.decode(data.asOpt[String].getOrElse(""))), "application/octet-stream")
        }
      }
    }
    val value   = bytes(json \ "value")
    val key     = bytes(json \ "key").flatMap((k, _) => utf8(k))
    val routing = (json \ "vendor_metadata").asOpt[Map[String, String]].getOrElse(Map.empty)
    val event   = defaults.copy(
      datacontenttype = value.map(_._2),
      data = value.map(_._1),
      extensions = defaults.extensions ++ key.map(k => "partitionkey" -> JsString(k))
    )
    (event, routing)
  }
}

/**
 * How a CloudEvent travels on each kind of broker, and how it is read back.
 *
 *  - kafka and pulsar: binary mode, the attributes in `ce_` headers (properties for pulsar), the data
 *    as the payload, its content type in `content-type`, `partitionkey` as the message key
 *  - rabbitmq: binary mode, the attributes in `cloudEvents:` application properties, the content type
 *    in the amqp content-type
 *  - mqtt 5: binary mode, the attributes as user properties (no prefix, as the mqtt binding says), the
 *    content type as the content type of the message
 *  - redis streams: binary mode, the attributes in `ce_` fields of the entry, next to its data field
 *  - redis pub/sub and mqtt 3.1.1, which carry no headers: structured mode, the whole event as json
 *
 * A message published without cloudevents (by another producer) is still read as an event: its id is
 * its position when the broker has one, its source `/<kind>/<topic>`, its type `MessageType`. Every
 * event read gets what the broker says of the message as extensions (`kafkaoffset`, `rabbitmqroutingkey`).
 */
object CloudEventBindings {

  import EventBrokerConnectionKind.*

  /** `headers`: whether the client can carry headers (mqtt 5 and redis streams can, mqtt 3.1.1 and redis pub/sub cannot) */
  def toMessage(event: CloudEvent, kind: EventBrokerConnectionKind, routing: Map[String, String] = Map.empty, headers: Boolean = true): BrokerMessage =
    kind match {
      case Kafka | Pulsar        => binary(event, "ce_", routing).copy(key = event.extension("partitionkey").map(ByteString(_)))
      case RabbitMQ              => withContentTypeProperty(binary(event, "cloudEvents:", routing), event)
      case MQTT if headers       => withContentTypeProperty(binary(event, "", routing), event)
      case Redis if headers      => binary(event, "ce_", routing)
      case Redis | MQTT          => BrokerMessage(data = ByteString(Json.stringify(event.json)), metadata = routing)
    }

  /** the protocols with a content type of their own (amqp, mqtt 5) */
  private def withContentTypeProperty(message: BrokerMessage, event: CloudEvent): BrokerMessage =
    message.copy(headers = message.headers - "content-type", metadata = message.metadata ++ event.datacontenttype.map(ct => "content_type" -> ct))

  private def binary(event: CloudEvent, prefix: String, routing: Map[String, String]): BrokerMessage = {
    val attributes = Map(
      "specversion" -> CloudEvent.SpecVersion,
      "id"          -> event.id,
      "source"      -> event.source,
      "type"        -> event.`type`
    ) ++ event.dataschema.map("dataschema" -> _) ++ event.subject.map("subject" -> _) ++ event.time.map("time" -> _) ++
      event.extensions.keys.flatMap(name => event.extension(name).map(name -> _))
    BrokerMessage(
      data = event.data.getOrElse(ByteString.empty),
      headers = attributes.map((name, value) => s"$prefix$name" -> value) ++ event.datacontenttype.map("content-type" -> _),
      metadata = routing
    )
  }

  def fromMessage(message: BrokerMessage, kind: EventBrokerConnectionKind): CloudEvent = {
    val event = kind match {
      case Kafka | Pulsar | Redis => fromHeaders(message, "ce_", message.headers.get("content-type"))
      case RabbitMQ               =>
        fromHeaders(message, "cloudEvents:", message.metadata.get("content_type"))
          .orElse(fromHeaders(message, "cloudEvents_", message.metadata.get("content_type")))
      case MQTT                   => fromHeaders(message, "", message.metadata.get("content_type"))
    }
    val withMetadata = (e: CloudEvent) => e.copy(extensions = brokerExtensions(message, kind) ++ e.extensions)
    withMetadata(event.orElse(structured(message, kind)).getOrElse(synthesized(message, kind)))
  }

  private def fromHeaders(message: BrokerMessage, prefix: String, contentType: Option[String]): Option[CloudEvent] = {
    val attributes = message.headers.collect { case (name, value) if name.startsWith(prefix) => name.drop(prefix.length) -> value }
    if (!attributes.get("specversion").contains(CloudEvent.SpecVersion)) None
    else
      Some(
        CloudEvent(
          id = attributes.getOrElse("id", UUID.randomUUID().toString),
          source = attributes.getOrElse("source", ""),
          `type` = attributes.getOrElse("type", CloudEvent.MessageType),
          datacontenttype = contentType,
          dataschema = attributes.get("dataschema"),
          subject = attributes.get("subject"),
          time = attributes.get("time"),
          data = Some(message.data),
          extensions = attributes
            .filterNot((name, _) => CloudEvent.ContextAttributes.contains(name))
            .filter((name, _) => CloudEvent.isValidName(name))
            .view
            .mapValues(JsString(_))
            .toMap ++ message.key.flatMap(CloudEvent.utf8).map(k => "partitionkey" -> JsString(k))
        )
      )
  }

  /** the whole event in the payload: what redis and mqtt carry, or what another producer chose to send */
  private def structured(message: BrokerMessage, kind: EventBrokerConnectionKind): Option[CloudEvent] = {
    val declared = kind match {
      case RabbitMQ | MQTT => message.metadata.get("content_type")
      case _               => message.headers.get("content-type")
    }
    val worthTrying = declared.exists(ct => CloudEvent.mediaType(ct) == CloudEvent.ContentType) ||
      (declared.isEmpty && message.data.headOption.contains('{'.toByte))
    if (!worthTrying) None
    else Try(Json.parse(message.data.toArray)).toOption.flatMap(json => CloudEvent.read(json).toOption)
  }

  private def topicOf(message: BrokerMessage, kind: EventBrokerConnectionKind): String = kind match {
    case Kafka | Pulsar | MQTT => message.metadata.getOrElse("topic", "")
    case RabbitMQ              => message.metadata.get("routing_key").filter(_.nonEmpty).orElse(message.metadata.get("exchange")).getOrElse("")
    case Redis                 => message.metadata.get("stream").orElse(message.metadata.get("channel")).getOrElse("")
  }

  private def synthesized(message: BrokerMessage, kind: EventBrokerConnectionKind): CloudEvent = {
    val id          = kind match {
      case Kafka  => s"${message.metadata.getOrElse("topic", "")}-${message.metadata.getOrElse("partition", "")}-${message.metadata.getOrElse("offset", "")}"
      case Pulsar => message.metadata.getOrElse("message_id", UUID.randomUUID().toString)
      case Redis if message.metadata.contains("stream") => s"${message.metadata.getOrElse("stream", "")}-${message.metadata.getOrElse("id", "")}"
      case _      => message.metadata.get("message_id").getOrElse(UUID.randomUUID().toString)
    }
    val millis      = kind match {
      case Kafka    => message.metadata.get("timestamp")
      case Pulsar   => message.metadata.get("publish_time")
      case RabbitMQ => message.metadata.get("timestamp")
      // the id of a stream entry starts with the time it was added
      case Redis    => message.metadata.get("id").map(_.takeWhile(_ != '-'))
      case _        => None
    }
    val declared    = kind match {
      case RabbitMQ | MQTT => message.metadata.get("content_type")
      case _               => message.headers.get("content-type")
    }
    // nothing says what the payload is: json if it reads as json, text if it reads as text
    val contentType = declared.orElse {
      if (message.data.isEmpty) None
      else if (Try(Json.parse(message.data.toArray)).isSuccess) Some("application/json")
      else if (CloudEvent.utf8(message.data).isDefined) Some("text/plain")
      else Some("application/octet-stream")
    }
    CloudEvent(
      id = id,
      source = s"/${kind.name}/${topicOf(message, kind)}",
      `type` = CloudEvent.MessageType,
      datacontenttype = contentType,
      time = millis.flatMap(_.toLongOption).filter(_ > 0).map(CloudEvent.time).orElse(Some(CloudEvent.now())),
      data = Some(message.data),
      extensions = message.key.flatMap(CloudEvent.utf8).map(k => "partitionkey" -> JsString(k)).toMap
    )
  }

  /** what the broker says of the message, as extensions named after the kind: `kafkaoffset`, `redischannel` */
  def brokerExtensions(message: BrokerMessage, kind: EventBrokerConnectionKind): Map[String, JsValue] =
    message.metadata.collect {
      case (name, value) if name != "provider" =>
        s"${kind.name}${name.filter(_.isLetterOrDigit).toLowerCase}" -> JsString(value)
    }
}
