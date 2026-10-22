package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

/** what a caller may ask, under the configuration of the plugin of its route (against kafka) */
class PolicySuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.kafka; () }

  lazy val connection: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers)))
  /** to read back what was published, wherever it went */
  lazy val reader             = Gateway.brokerRoute[HttpEventsConsumer](connection)

  def read(topic: String): JsValue =
    await(collect(Gateway.ndjson(reader, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 1)).head

  test("by default a caller cannot choose the topic: the route does") {
    val topic           = uniq("fixed")
    val ingester        = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("options" -> Json.obj("topic" -> topic)))
    val (refused, why)  = Gateway.post(ingester, "/" + opts("topic" -> uniq("elsewhere")), Json.stringify(message(0, "x")))
    assertEquals(refused, 400, why.toString)
    assert(why.toString.contains("cannot set 'topic'"), why.toString)
    val (status, lines) = Gateway.post(ingester, "/", Json.stringify(message(1, "x")))
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "details" \ "topic").as[String], topic)
    assertEquals(numbers(Seq(read(topic))), Seq(1))
  }

  test("what only shapes the response stays open to the caller") {
    val topic    = uniq("shaping")
    val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection, Json.obj("options" -> Json.obj("topic" -> topic, "from" -> "earliest")))
    val ingester = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("options" -> Json.obj("topic" -> topic)))
    (0 until 3).foreach(n => Gateway.post(ingester, "/", Json.stringify(message(n, "s"))))
    val lines    = await(collect(Gateway.lines(consumer, "/" + opts("take" -> 2, "format" -> "raw")).filter(_.nonEmpty), take = 5, within = 5.seconds))
    assertEquals(lines.map(l => Json.parse(l)), Seq(message(0, "s"), message(1, "s")))
    // and nothing else
    val res      = await(Gateway.request(consumer, path = "/" + opts("group" -> "someone-else")))
    res.discardEntityBytes()
    assertEquals(res.status.intValue(), 400)
  }

  test("a caller allowed to name the topic is held to the patterns of the route") {
    val ingester = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("allowed_overrides" -> Json.arr("topic"), "allowed_topics" -> Json.arr("orders-*")))
    val allowed  = s"orders-${uniq("eu")}"
    assertEquals(Gateway.post(ingester, "/" + opts("topic" -> allowed), Json.stringify(message(0, "p")))._1, 200)
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> "payroll"), Json.stringify(message(0, "p")))
    assertEquals(status, 400, lines.toString)
    assert(lines.toString.contains("cannot use 'payroll'"), lines.toString)
  }

  test("the options of the route may hold expressions, evaluated for each call") {
    val ingester        = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("options" -> Json.obj("topic" -> "tenant-${req.headers.x-tenant}")))
    val tenant          = uniq("acme")
    val (status, lines) = Gateway.post(ingester, "/", Json.stringify(message(0, "t")), headers = Seq("X-Tenant" -> tenant))
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "details" \ "topic").as[String], s"tenant-$tenant")
  }

  test("what a caller sends is never evaluated") {
    val topic    = uniq("literal")
    val ingester = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("options" -> Json.obj("topic" -> topic), "allowed_overrides" -> Json.arr("source")))
    val (status, lines) = Gateway.post(ingester, "/" + opts("source" -> "${req.headers.x-secret}"), Json.stringify(message(0, "l")), headers = Seq("X-Secret" -> "s3cr3t"))
    assertEquals(status, 200, lines.toString)
    assertEquals((read(topic) \ "source").as[String], "${req.headers.x-secret}")
  }

  test("where an event says it goes is held to the same rules") {
    val topic           = uniq("routed")
    val ingester        = Gateway.brokerRoute[HttpEventsIngester](connection, Json.obj("options" -> Json.obj("topic" -> topic)))
    val events          = Json.obj("events" -> Json.arr(
      Json.obj("value" -> Json.obj("type" -> "json", "data" -> message(0, "r"))),
      Json.obj("value" -> Json.obj("type" -> "json", "data" -> message(1, "r")), "vendor_metadata" -> Json.obj("topic" -> "elsewhere"))
    ))
    val (status, lines) = Gateway.post(ingester, "/", Json.stringify(events), ctype = "application/vnd.cloud-apim-event-broker+json")
    assertEquals(status, 200, lines.toString)
    assertEquals(lines.map(l => (l \ "published").as[Boolean]), Seq(true, false))
    assert((lines(1) \ "error_description").as[String].contains("cannot set 'topic'"), lines.toString)
  }
}
