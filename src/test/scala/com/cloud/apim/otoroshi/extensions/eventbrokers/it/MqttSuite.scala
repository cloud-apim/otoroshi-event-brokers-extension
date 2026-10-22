package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import java.io.File
import scala.concurrent.duration.*

class MqttSuite extends BrokerSuite {

  override def startBroker(): Unit = { Brokers.mosquitto; () }

  // paho keeps its in-flight messages in a folder named after the client id, in the working
  // directory: a recognisable id lets the suite clean up behind it
  val clientId = uniq("it-mqtt")

  lazy val connection: String = Gateway.createConnection(
    "mqtt",
    Json.obj(
      "host"         -> Brokers.mosquitto.getHost,
      "port"         -> Brokers.mosquittoPort,
      "publisher_id" -> clientId,
      "topic"        -> "default-topic"
    )
  )
  lazy val ingester = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection)

  override def afterAll(): Unit = {
    def rm(f: File): Unit = { Option(f.listFiles()).toSeq.flatten.foreach(rm); f.delete(); () }
    Option(new File(System.getProperty("user.dir")).listFiles()).toSeq.flatten.filter(_.getName.startsWith(clientId)).foreach(rm)
  }

  def publish(topic: String, n: Int, run: String, extra: (String, Any)*): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts((Seq("topic" -> topic) ++ extra)*), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  test("publishes on a topic") {
    val res = publish(uniq("publish"), 0, uniq("run"), "retained" -> false)
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
  }

  test("http consumer: what is published comes back, in order") {
    val topic    = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3)
    keepPublishing(received)(n => publish(topic, n, run, "retained" -> false))
    val events   = await(received)
    assertConsecutive(numbers(events), 3)
    assertEquals((events.head \ "vendor_metadata" \ "topic").asOpt[String], Some(topic))
  }

  // `retained` defaults to true: every message published becomes the topic's retained message and
  // is replayed to whoever subscribes later
  test("does not retain what it publishes unless asked to".fail) {
    val topic    = uniq("retained")
    publish(topic, 0, uniq("run"))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 1, within = 3.seconds))
    assertEquals(received, Seq.empty)
  }
}
