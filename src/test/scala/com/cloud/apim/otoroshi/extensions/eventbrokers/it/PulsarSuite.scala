package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class PulsarSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.pulsar; () }

  lazy val connection: String = Gateway.createConnection(
    "pulsar",
    Json.obj(
      "service_url"   -> Brokers.pulsar.getPulsarBrokerUrl,
      "tenant"        -> "public",
      "namespace"     -> "default",
      "default_topic" -> "default-topic"
    )
  )
  lazy val ingester = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection)
  lazy val sse      = Gateway.brokerRoute[SseEventConsumer](connection)

  def publish(topic: String, n: Int, run: String): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  /** the broker's own view of a topic: producers, subscriptions, counters */
  def stats(topic: String): JsValue = {
    val res = await(Gateway.ws.url(s"${Brokers.pulsar.getHttpServiceUrl}/admin/v2/persistent/public/default/$topic/stats").get())
    assertEquals(res.status, 200, res.body)
    res.json
  }

  test("publishes to a topic") {
    val res = publish(uniq("publish"), 0, uniq("run"))
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
  }

  // the consumer starts from the latest message: keep publishing until it is subscribed.
  // the first message received kills the stream: its schema version is null (no schema on the
  // topic) and goes straight into a ByteString
  test("http consumer: what is published comes back, in order, with the pulsar metadata".fail) {
    val topic    = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3, within = 60.seconds)
    keepPublishing(received, atMost = 60.seconds)(n => publish(topic, n, run))
    val events   = await(received)
    assertConsecutive(numbers(events), 3)
    assert((events.head \ "vendor_metadata" \ "topic").as[String].endsWith(topic), events.head.toString)
  }

  // same null schema version as above
  test("sse consumer: one event per message".fail) {
    val topic    = uniq("sse")
    val run      = uniq("run")
    val received = collect(Gateway.sse(sse, "/" + opts("topic" -> topic)), take = 3, within = 60.seconds)
    keepPublishing(received, atMost = 60.seconds)(n => publish(topic, n, run))
    assertConsecutive(numbers(await(received).map(e => Json.parse(e.data))), 3)
  }

  // a new producer per publish, never closed
  test("reuses its producer from one publish to the next".fail) {
    val topic = uniq("producers")
    val run   = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    val producers = (stats(topic) \ "publishers").as[JsArray].value.size
    assert(producers <= 1, s"$producers producers on the topic")
  }

  // every consumer uses the same exclusive subscription, so a second one is refused
  test("serves two consumers of the same topic at once".fail) {
    val topic  = uniq("concurrent")
    val path   = "/" + opts("topic" -> topic)
    val first  = collect(Gateway.ndjson(consumer, path), take = 1, within = 15.seconds)
    eventually("the first consumer on the subscription") {
      (stats(topic) \ "subscriptions" \ "otoroshi" \ "consumers").as[JsArray].value.nonEmpty
    }
    val second = await(Gateway.request(consumer, path = path))
    second.discardEntityBytes()
    assertEquals(second.status.intValue(), 200)
    await(first)
  }
}
