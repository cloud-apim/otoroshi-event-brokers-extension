package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class MqttSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.mosquitto; () }

  lazy val connection: String = Gateway.createConnection(
    "mqtt",
    Json.obj(
      "host"          -> Brokers.mosquitto.getHost,
      "port"          -> Brokers.mosquittoPort,
      "default_topic" -> "default-topic"
    )
  )
  lazy val ingester = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection)

  def publish(topic: String, n: Int, run: String, extra: (String, Any)*): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts((Seq("topic" -> topic) ++ extra)*), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  test("publishes on a topic") {
    val res = publish(uniq("publish"), 0, uniq("run"))
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
  }

  test("http consumer: what is published comes back, in order") {
    val topic    = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3)
    keepPublishing(received)(n => publish(topic, n, run))
    val events   = await(received)
    assertConsecutive(numbers(events), 3)
    assertEquals((events.head \ "vendor_metadata" \ "topic").asOpt[String], Some(topic))
  }

  test("does not retain what it publishes unless asked to") {
    val topic    = uniq("retained")
    publish(topic, 0, uniq("run"))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 1, within = 3.seconds))
    assertEquals(received, Seq.empty)
  }

  test("retains when the connection says so") {
    val retaining = Gateway.createConnection(
      "mqtt",
      Json.obj("host" -> Brokers.mosquitto.getHost, "port" -> Brokers.mosquittoPort, "retain" -> true)
    )
    val ingesting = Gateway.brokerRoute[HttpEventsIngester](retaining)
    val topic     = uniq("retaining")
    val run       = uniq("run")
    val (status, lines) = Gateway.post(ingesting, "/" + opts("topic" -> topic), Json.stringify(message(0, run)))
    assertEquals(status, 200, lines.toString)
    // a late subscriber gets the retained message at once
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 1, within = 10.seconds))
    assertEquals(numbers(received), Seq(0))
    Gateway.deleteConnection(retaining)
  }

  test("two subscribers of a topic both get every message, and one leaving does not cut the other") {
    val topic = uniq("shared")
    val run   = uniq("run")
    val path  = "/" + opts("topic" -> topic)
    val short = collect(Gateway.ndjson(consumer, path), take = 2)
    val long  = collect(Gateway.ndjson(consumer, path), take = 6)
    keepPublishing(short.zip(long))(n => publish(topic, n, run))
    assertConsecutive(numbers(await(short)), 2)
    assertConsecutive(numbers(await(long)), 6)
  }

  test("a group is refused") {
    val res  = await(Gateway.request(consumer, path = "/" + opts("topic" -> uniq("group"), "group" -> "g")))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
  }
}
