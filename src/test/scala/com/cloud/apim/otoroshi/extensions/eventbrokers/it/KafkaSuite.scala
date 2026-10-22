package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.stream.scaladsl.Source
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import java.util.Properties
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class KafkaSuite extends BrokerSuite {

  override def startBroker(): Unit = { Brokers.kafka; () }

  lazy val connection: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers), "default_topic" -> "default-topic"))
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)
  lazy val sse                = Gateway.brokerRoute[SseEventConsumer](connection)
  lazy val websocket          = Gateway.brokerRoute[IngesterConsumerWebsocket](connection, Json.obj("in" -> Json.obj("ref" -> connection), "out" -> Json.obj("ref" -> connection)))

  def createTopic(name: String, partitions: Int): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Brokers.kafka.getBootstrapServers)
    val admin = AdminClient.create(props)
    try admin.createTopics(Seq(new NewTopic(name, partitions, 1.toShort)).asJava).all().get()
    finally admin.close()
  }

  def publish(topic: String, n: Int, run: String, extra: (String, Any)*): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts((Seq("topic" -> topic) ++ extra)*), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  test("publishes over http and answers with the record metadata") {
    val topic = uniq("publish")
    val res   = publish(topic, 0, "r")
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
    assertEquals((res \ "details" \ "topic").asOpt[String], Some(topic))
    assertEquals((res \ "details" \ "offset").asOpt[Long], Some(0L))
  }

  test("http consumer: what is published comes back, in order, with the kafka metadata") {
    val topic    = uniq("http")
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(topic, n, run))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 3))
    assertEquals(numbers(received), Seq(0, 1, 2))
    assertEquals(received.map(e => (e \ "vendor_metadata" \ "topic").as[String]).distinct, Seq(topic))
    assertEquals(received.map(e => (e \ "vendor_metadata" \ "offset").as[String]), Seq("0", "1", "2"))
  }

  test("sse consumer: one event per message") {
    val topic    = uniq("sse")
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(topic, n, run))
    val received = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 3))
    assertEquals(numbers(received.map(e => Json.parse(e.data))), Seq(0, 1, 2))
  }

  test("websocket: what is sent in comes back out") {
    val topic    = uniq("ws")
    val run      = uniq("run")
    val out      = Source((0 until 3).toList.map(n => TextMessage(Json.stringify(message(n, run)))))
    val path     = "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest", "content-type" -> "application/json")
    val received = await(Gateway.websocket(websocket, path, out, take = 3, within = 30.seconds))
    assertEquals(numbers(received.map(Json.parse)), Seq(0, 1, 2))
  }

  test("text bodies keep their content type even with a charset".fail) {
    val topic           = uniq("text")
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), "hello", ctype = "text/plain; charset=UTF-8")
    assertEquals(status, 200, lines.toString)
    val received        = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 1))
    assertEquals((received.head \ "value" \ "type").asOpt[String], Some("text"), received.toString)
  }

  // the partitioned source is flattened with flatMapConcat: partition sources never complete, so
  // only the first one assigned is ever read
  test("consumes every partition of a topic".fail) {
    val topic    = uniq("partitions")
    val run      = uniq("run")
    createTopic(topic, 3)
    (0 until 3).foreach(n => publish(topic, n, run, "partition" -> n))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 3, within = 20.seconds))
    assertEquals(numbers(received).sorted, Seq(0, 1, 2))
  }
}
