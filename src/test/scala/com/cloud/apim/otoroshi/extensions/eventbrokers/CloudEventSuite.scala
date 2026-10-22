package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnectionKind
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{Output, OutputFormat, SseIds}
import org.apache.pekko.util.ByteString
import play.api.libs.json.*

import java.util.Base64

class CloudEventSuite extends munit.FunSuite {

  val event = CloudEvent(
    id = "e1",
    source = "/orders",
    `type` = "com.acme.order.created",
    datacontenttype = Some("application/json"),
    subject = Some("o-42"),
    time = Some("2026-10-22T10:00:00Z"),
    data = Some(ByteString("""{"n":1}""")),
    extensions = Map("partitionkey" -> JsString("o-42"), "tenant" -> JsString("acme"))
  )

  test("structured: json data goes out as json, and reads back to the same event") {
    val json = event.json
    assertEquals((json \ "specversion").as[String], "1.0")
    assertEquals((json \ "data").as[JsValue], Json.obj("n" -> 1))
    assertEquals((json \ "tenant").as[String], "acme")
    assertEquals(CloudEvent.read(json), Right(event))
  }

  test("structured: text goes out as a string, anything else as base64") {
    val text   = event.copy(datacontenttype = Some("text/plain; charset=UTF-8"), data = Some(ByteString("hello")))
    assertEquals((text.json \ "data").as[String], "hello")
    assertEquals(CloudEvent.read(text.json), Right(text))
    val binary = event.copy(datacontenttype = Some("application/octet-stream"), data = Some(ByteString(Array[Byte](0, 1, 2))))
    assertEquals((binary.json \ "data_base64").as[String], Base64.getEncoder.encodeToString(Array[Byte](0, 1, 2)))
    assertEquals(CloudEvent.read(binary.json), Right(binary))
    // json that does not parse is still carried, as base64
    val broken = event.copy(data = Some(ByteString("{nope")))
    assert((broken.json \ "data_base64").isDefined)
  }

  test("structured: no datacontenttype means json") {
    val read = CloudEvent.read(Json.obj("specversion" -> "1.0", "id" -> "1", "source" -> "/s", "type" -> "t", "data" -> Json.obj("a" -> 1)))
    assertEquals(read.map(_.data.map(_.utf8String)), Right(Some("""{"a":1}""")))
  }

  test("structured: what is not a cloudevent is refused, saying why") {
    def errors(json: JsValue) = CloudEvent.read(json).left.getOrElse("")
    assert(errors(Json.obj("specversion" -> "0.3", "id" -> "1", "source" -> "s", "type" -> "t")).contains("specversion must be 1.0"))
    assert(errors(Json.obj("specversion" -> "1.0", "source" -> "s", "type" -> "t")).contains("id is required"))
    assert(errors(Json.obj("specversion" -> "1.0", "id" -> "1", "type" -> "t")).contains("source is required"))
    assert(errors(Json.obj("specversion" -> "1.0", "id" -> "1", "source" -> "s")).contains("type is required"))
    val valid = Json.obj("specversion" -> "1.0", "id" -> "1", "source" -> "s", "type" -> "t")
    assert(errors(valid ++ Json.obj("Bad-Name" -> "x")).contains("not a valid attribute name"))
    assert(errors(valid ++ Json.obj("nested" -> Json.obj("a" -> 1))).contains("must be a string, a number or a boolean"))
    assert(errors(valid ++ Json.obj("data" -> "a", "data_base64" -> "YQ==")).contains("cannot be both there"))
    assert(errors(Json.arr()).contains("json object"))
  }

  test("batch: every event, or the first one at fault") {
    val ok = CloudEvent.readBatch(Json.arr(event.json, event.copy(id = "e2").json))
    assertEquals(ok.map(_.map(_.id)), Right(Seq("e1", "e2")))
    assert(CloudEvent.readBatch(Json.arr(event.json, Json.obj("specversion" -> "1.0"))).left.exists(_.startsWith("event 1:")))
  }

  test("http binary mode: attributes in ce- headers, the content type as it was sent") {
    val defaults = CloudEvent.generated("/otoroshi/routes/r1")
    val read     = CloudEvent.fromHttp(
      Map("Ce-Id" -> "abc", "ce-type" -> "com.acme.x", "ce-tenant" -> "acme", "Content-Type" -> "text/plain; charset=UTF-8", "Host" -> "h"),
      ByteString("hello"),
      defaults
    )
    assertEquals(read.map(e => (e.id, e.source, e.`type`, e.datacontenttype, e.extensions)), Right(("abc", "/otoroshi/routes/r1", "com.acme.x", Some("text/plain; charset=UTF-8"), Map("tenant" -> JsString("acme")))))
    assert(CloudEvent.fromHttp(Map("ce-specversion" -> "0.3"), ByteString.empty, defaults).isLeft)
    assert(CloudEvent.fromHttp(Map("ce-Bad_Name" -> "x"), ByteString.empty, defaults).isLeft)
  }

  test("the format of the extension before cloudevents is still read") {
    val (read, routing) = CloudEvent.fromLegacy(
      Json.obj(
        "key"             -> Json.obj("type" -> "text", "data" -> "k1"),
        "value"           -> Json.obj("type" -> "json", "data" -> Json.obj("a" -> 1)),
        "vendor_metadata" -> Json.obj("topic" -> "t")
      ),
      CloudEvent.generated("/s")
    )
    assertEquals(read.datacontenttype, Some("application/json"))
    assertEquals(read.data.map(_.utf8String), Some("""{"a":1}"""))
    assertEquals(read.extension("partitionkey"), Some("k1"))
    assertEquals(routing, Map("topic" -> "t"))
  }

  def roundTrip(kind: EventBrokerConnectionKind, metadata: Map[String, String]): CloudEvent = {
    val message = CloudEventBindings.toMessage(event, kind)
    CloudEventBindings.fromMessage(message.copy(metadata = metadata), kind)
  }

  test("kafka and pulsar: binary mode, ce_ headers, partitionkey as the key") {
    val message = CloudEventBindings.toMessage(event, EventBrokerConnectionKind.Kafka)
    assertEquals(message.headers.get("ce_id"), Some("e1"))
    assertEquals(message.headers.get("ce_specversion"), Some("1.0"))
    assertEquals(message.headers.get("ce_tenant"), Some("acme"))
    assertEquals(message.headers.get("content-type"), Some("application/json"))
    assertEquals(message.key.map(_.utf8String), Some("o-42"))
    assertEquals(message.data.utf8String, """{"n":1}""")
    Seq(EventBrokerConnectionKind.Kafka, EventBrokerConnectionKind.Pulsar).foreach { kind =>
      val back = roundTrip(kind, Map("topic" -> "orders"))
      assertEquals(back.copy(extensions = back.extensions - s"${kind.name}topic"), event, kind.name)
      assertEquals(back.extension(s"${kind.name}topic"), Some("orders"))
    }
  }

  test("rabbitmq: binary mode, cloudEvents: application properties, the amqp content type") {
    val message = CloudEventBindings.toMessage(event, EventBrokerConnectionKind.RabbitMQ, Map("queue" -> "q"))
    assertEquals(message.headers.get("cloudEvents:id"), Some("e1"))
    assertEquals(message.headers.get("content-type"), None)
    assertEquals(message.metadata, Map("queue" -> "q", "content_type" -> "application/json"))
    val back    = CloudEventBindings.fromMessage(message.copy(metadata = Map("content_type" -> "application/json", "routing_key" -> "q")), EventBrokerConnectionKind.RabbitMQ)
    assertEquals(back.copy(extensions = back.extensions -- Seq("rabbitmqcontenttype", "rabbitmqroutingkey", "partitionkey")), event.copy(extensions = event.extensions - "partitionkey"))
  }

  test("mqtt 5: binary mode, the attributes as user properties, the content type as the mqtt 5 one") {
    val message = CloudEventBindings.toMessage(event, EventBrokerConnectionKind.MQTT, Map("topic" -> "t"), headers = true)
    assertEquals(message.headers.get("id"), Some("e1"))
    assertEquals(message.headers.get("specversion"), Some("1.0"))
    assertEquals(message.headers.get("content-type"), None)
    assertEquals(message.metadata, Map("topic" -> "t", "content_type" -> "application/json"))
    val back    = CloudEventBindings.fromMessage(message.copy(metadata = Map("content_type" -> "application/json", "topic" -> "t")), EventBrokerConnectionKind.MQTT)
    assertEquals(back.copy(extensions = back.extensions -- Seq("mqttcontenttype", "mqtttopic")), event)
  }

  test("redis and mqtt 3.1.1: structured mode, the whole event in the payload") {
    Seq(EventBrokerConnectionKind.Redis, EventBrokerConnectionKind.MQTT).foreach { kind =>
      val message = CloudEventBindings.toMessage(event, kind, headers = false)
      assertEquals(message.headers, Map.empty[String, String])
      assertEquals(CloudEvent.read(Json.parse(message.data.toArray)), Right(event))
      val back    = roundTrip(kind, Map("channel" -> "c", "topic" -> "t"))
      assertEquals(back.id, "e1")
      assertEquals(back.data, event.data)
    }
  }

  test("a message published without cloudevents is read as one: its position, its topic, what it looks like") {
    val kafka = CloudEventBindings.fromMessage(
      BrokerMessage(data = ByteString("""{"a":1}"""), metadata = Map("topic" -> "orders", "partition" -> "2", "offset" -> "7", "timestamp" -> "1792663171364")),
      EventBrokerConnectionKind.Kafka
    )
    assertEquals(kafka.id, "orders-2-7")
    assertEquals(kafka.source, "/kafka/orders")
    assertEquals(kafka.`type`, CloudEvent.MessageType)
    assertEquals(kafka.datacontenttype, Some("application/json"))
    assertEquals(kafka.time, Some(CloudEvent.time(1792663171364L)))
    assertEquals(kafka.extension("kafkaoffset"), Some("7"))
    val text  = CloudEventBindings.fromMessage(BrokerMessage(data = ByteString("hello"), metadata = Map("channel" -> "c")), EventBrokerConnectionKind.Redis)
    assertEquals((text.source, text.datacontenttype), ("/redis/c", Some("text/plain")))
    val bytes = CloudEventBindings.fromMessage(BrokerMessage(data = ByteString(Array[Byte](-1, -2))), EventBrokerConnectionKind.MQTT)
    assertEquals(bytes.datacontenttype, Some("application/octet-stream"))
  }

  test("sse: every line of the data gets its own data field") {
    val multiline = event.copy(datacontenttype = Some("text/plain"), data = Some(ByteString("line 1\nline 2\r\nline 3")))
    assertEquals(
      Output.sse(multiline, OutputFormat.Raw, "id-1").utf8String,
      "id: id-1\nevent: com.acme.order.created\ndata: line 1\ndata: line 2\ndata: line 3\n\n"
    )
    // an id cannot hold a new line
    assert(Output.sse(event, OutputFormat.CloudEvents, "a\nb").utf8String.startsWith("id: a b\n"))
    // the structured event is one json line
    assertEquals(Output.sse(event, OutputFormat.CloudEvents, "x").utf8String.count(_ == '\n'), 4)
  }

  test("sse: kafka ids carry the positions read, and give them back") {
    val positions = Map("orders:0" -> 4L, "orders:1" -> 9L)
    assertEquals(SseIds.readKafka(SseIds.kafka(positions)), Some(positions))
    assertEquals(SseIds.readKafka("e1"), None)
    assertEquals(SseIds.resume(EventBrokerConnectionKind.Kafka, Some(SseIds.kafka(positions))), Some(StartFrom.Positions(positions)))
    assertEquals(SseIds.resume(EventBrokerConnectionKind.Redis, Some(SseIds.kafka(positions))), None)
  }

  test("the output formats") {
    assertEquals(OutputFormat.from(Json.obj()), Right(OutputFormat.CloudEvents))
    assertEquals(OutputFormat.from(Json.obj("format" -> "raw")), Right(OutputFormat.Raw))
    assertEquals(OutputFormat.from(Json.obj("enriched" -> "false")), Right(OutputFormat.Raw))
    assert(OutputFormat.from(Json.obj("format" -> "xml")).isLeft)
    assertEquals(Output.ndjson(event, OutputFormat.Raw).utf8String, "{\"n\":1}\n")
  }
}
