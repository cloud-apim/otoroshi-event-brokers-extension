package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*
import play.api.libs.ws.JsonBodyWritables.writeableOf_JsValue

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

  // the consumer starts from the latest message: keep publishing until it is subscribed
  test("http consumer: what is published comes back, in order, with the pulsar metadata") {
    val topic    = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3, within = 60.seconds)
    keepPublishing(received, atMost = 60.seconds)(n => publish(topic, n, run))
    val events   = await(received)
    assertConsecutive(numbers(events), 3)
    assert(ext(events.head, "pulsartopic").exists(_.endsWith(topic)), events.head.toString)
  }

  test("sse consumer: one event per message") {
    val topic    = uniq("sse")
    val run      = uniq("run")
    val received = collect(Gateway.sse(sse, "/" + opts("topic" -> topic)), take = 3, within = 60.seconds)
    keepPublishing(received, atMost = 60.seconds)(n => publish(topic, n, run))
    assertConsecutive(numbers(await(received).map(e => Json.parse(e.data))), 3)
  }

  test("reuses its producer from one publish to the next") {
    val topic = uniq("producers")
    val run   = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    val producers = (stats(topic) \ "publishers").as[JsArray].value.size
    assert(producers <= 1, s"$producers producers on the topic")
  }

  test("serves two consumers of the same topic at once") {
    val topic  = uniq("concurrent")
    val path   = "/" + opts("topic" -> topic)
    val first  = collect(Gateway.ndjson(consumer, path), take = 1, within = 3.seconds)
    eventually("the first consumer subscribed") {
      (stats(topic) \ "subscriptions").as[JsObject].values.exists(s => (s \ "consumers").as[JsArray].value.nonEmpty)
    }
    val second = await(Gateway.request(consumer, path = path))
    second.discardEntityBytes()
    assertEquals(second.status.intValue(), 200)
    await(first)
  }

  def consumersOf(topic: String, subscription: String): Int =
    (stats(topic) \ "subscriptions" \ subscription \ "consumers").asOpt[JsArray].map(_.value.size).getOrElse(0)

  test("the subscribers of a group share the messages") {
    val topic = uniq("shared")
    val group = uniq("group")
    val run   = uniq("run")
    val path  = "/" + opts("topic" -> topic, "group" -> group)
    val one   = collect(Gateway.ndjson(consumer, path), take = 6, within = 5.seconds)
    val other = collect(Gateway.ndjson(consumer, path), take = 6, within = 5.seconds)
    eventually("both subscribed")(consumersOf(topic, group) == 2)
    (0 until 6).foreach(n => publish(topic, n, run))
    val all   = numbers(await(one)) ++ numbers(await(other))
    assertEquals(all.sorted, (0 until 6).toSeq)
  }

  test("a group keeps what is published while none of its subscribers is connected") {
    val topic = uniq("durable")
    val group = uniq("group")
    val run   = uniq("run")
    // the first subscriber creates the durable subscription of the group, and goes
    await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "group" -> group)), take = 1, within = 2.seconds))
    eventually("the first subscriber gone")(consumersOf(topic, group) == 0)
    (0 until 2).foreach(n => publish(topic, n, run))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "group" -> group)), take = 2))
    assertEquals(numbers(received), Seq(0, 1))
  }

  // `take` on the gateway's side: the events that went out are the ones acked
  test("at least once in a group: what was not acked comes back to the next subscriber") {
    val topic = uniq("redelivered")
    val group = uniq("group")
    val run   = uniq("run")
    val path  = (take: Int) => "/" + opts("topic" -> topic, "group" -> group, "delivery" -> "at-least-once", "take" -> take)
    val first = collect(Gateway.ndjson(consumer, path(1)), take = 1)
    eventually("subscribed")(consumersOf(topic, group) == 1)
    (0 until 3).foreach(n => publish(topic, n, run))
    assertEquals(numbers(await(first)), Seq(0))
    eventually("the first subscriber gone")(consumersOf(topic, group) == 0)
    val rest  = await(collect(Gateway.ndjson(consumer, path(2)), take = 2))
    assertEquals(numbers(rest).sorted, Seq(1, 2))
  }

  test("sse: a client coming back resumes right after the last message it got") {
    val topic = uniq("sse-resume")
    val run   = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    val first = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic, "from" -> "earliest", "take" -> 2)).filter(_.id.isDefined), take = 2))
    assertEquals(numbers(first.map(e => Json.parse(e.data))), Seq(0, 1))
    assert(first.last.id.exists(_.startsWith("pulsar:")), first.toString)
    val rest  = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic), Seq("Last-Event-ID" -> first.last.id.get)).filter(_.id.isDefined), take = 3))
    assertEquals(numbers(rest.map(e => Json.parse(e.data))), Seq(2, 3, 4))
  }

  test("sse: resuming a partitioned topic, partition by partition") {
    val topic = uniq("sse-partitioned")
    val run   = uniq("run")
    val res   = await(Gateway.ws.url(s"${Brokers.pulsar.getHttpServiceUrl}/admin/v2/persistent/public/default/$topic/partitions").addHttpHeaders("Content-Type" -> "application/json").put(Json.toJson(3): JsValue))
    assert(res.status < 300, res.body)
    (0 until 9).foreach(n => publish(topic, n, run))
    val first = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic, "from" -> "earliest", "take" -> 4)).filter(_.id.isDefined), take = 4))
    // the same url, as EventSource does: a partition it got nothing from yet starts from the earliest too
    val rest  = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic, "from" -> "earliest"), Seq("Last-Event-ID" -> first.last.id.get)).filter(_.id.isDefined), take = 5, within = 15.seconds))
    // every message once, wherever it was
    assertEquals((numbers(first.map(e => Json.parse(e.data))) ++ numbers(rest.map(e => Json.parse(e.data)))).sorted, (0 until 9).toSeq)
  }

  test("an offset is not a pulsar position: refused") {
    val res = await(Gateway.request(consumer, path = "/" + opts("topic" -> uniq("offset"), "offset_from" -> 3)))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
  }
}
