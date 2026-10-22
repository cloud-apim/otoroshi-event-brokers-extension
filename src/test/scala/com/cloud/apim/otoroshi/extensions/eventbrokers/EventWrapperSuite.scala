package com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.util.ByteString
import play.api.libs.json.*

import java.util.Base64

class EventWrapperSuite extends munit.FunSuite {

  def event(data: String, dataType: String = "text", key: Option[String] = None): EventWrapper =
    EventWrapper(key.map(ByteString(_)), ByteString(data), "text", dataType, Map.empty, Map.empty)

  test("reads text, json and base64 values from the vendor json format") {
    val text   = EventWrapper.fromJson(Json.obj("value" -> Json.obj("type" -> "text", "data" -> "hello")))
    val json   = EventWrapper.fromJson(Json.obj("value" -> Json.obj("type" -> "json", "data" -> Json.obj("a" -> 1))))
    val binary = EventWrapper.fromJson(
      Json.obj("value" -> Json.obj("type" -> "binary", "data" -> Base64.getEncoder.encodeToString("bytes".getBytes)))
    )
    assertEquals(text.data.utf8String, "hello")
    assertEquals(Json.parse(json.data.utf8String), Json.obj("a" -> 1))
    assertEquals(binary.data.utf8String, "bytes")
  }

  test("reads the key, the headers and the vendor metadata") {
    val e = EventWrapper.fromJson(
      Json.obj(
        "key"             -> Json.obj("type" -> "text", "data" -> "k1"),
        "value"           -> Json.obj("type" -> "text", "data" -> "v"),
        "headers"         -> Json.obj("h" -> "1"),
        "vendor_metadata" -> Json.obj("topic" -> "t")
      )
    )
    assertEquals(e.key.map(_.utf8String), Some("k1"))
    assertEquals(e.headers, Map("h" -> "1"))
    assertEquals(e.vendorMetadata, Map("topic" -> "t"))
  }

  test("writes back what it read") {
    val in  = Json.obj(
      "key"             -> Json.obj("type" -> "text", "data" -> "k1"),
      "value"           -> Json.obj("type" -> "json", "data" -> Json.obj("a" -> Json.arr(1, 2))),
      "headers"         -> Json.obj("h" -> "1"),
      "vendor_metadata" -> Json.obj("topic" -> "t")
    )
    val out = EventWrapper.fromJson(in).json
    assertEquals(out \ "key", in \ "key")
    assertEquals(out \ "value", in \ "value")
    assertEquals(out \ "headers", in \ "headers")
    assertEquals(out \ "vendor_metadata", in \ "vendor_metadata")
  }

  test("binary values go out as base64") {
    val out = event("bytes", dataType = "binary").json
    assertEquals((out \ "value" \ "data").as[String], Base64.getEncoder.encodeToString("bytes".getBytes))
  }

  test("sse: the key becomes the event id") {
    assertEquals(event("v", key = Some("k1")).sseSerialize(enriched = false).utf8String, "id: k1\ndata: v\n\n")
  }

  // every line of a multi-line payload needs its own `data:` prefix, or the client reads the rest
  // as unknown fields and the event comes out truncated
  test("sse: a multi-line payload stays one event".fail) {
    assertEquals(event("line 1\nline 2").sseSerialize(enriched = false).utf8String, "data: line 1\ndata: line 2\n\n")
  }
}
