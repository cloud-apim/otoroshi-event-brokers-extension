package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

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
    assertEquals(ext(events.head, "mqtttopic"), Some(topic))
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

  test("mqtt 3.1.1: a group is refused, shared subscriptions are mqtt 5") {
    val v3   = Gateway.createConnection("mqtt", Json.obj("host" -> Brokers.mosquitto.getHost, "port" -> Brokers.mosquittoPort, "version" -> 3))
    val res  = await(Gateway.request(Gateway.brokerRoute[HttpEventsConsumer](v3), path = "/" + opts("topic" -> uniq("group"), "group" -> "g")))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    Gateway.deleteConnection(v3)
  }

  /** a plain mqtt client, outside of otoroshi, collecting what it receives on the topic */
  def native5(topic: String): (java.util.concurrent.LinkedBlockingQueue[com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish], () => Unit) = {
    import com.hivemq.client.mqtt.MqttClient
    import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe
    val client   = MqttClient.builder().useMqttVersion5().identifier(uniq("native")).serverHost(Brokers.mosquitto.getHost).serverPort(Brokers.mosquittoPort).buildAsync()
    client.connect().get()
    val received = new java.util.concurrent.LinkedBlockingQueue[com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish]()
    client.subscribe(Mqtt5Subscribe.builder().topicFilter(topic).build(), (p: com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish) => { received.add(p); () }).get()
    (received, () => { client.disconnect(); () })
  }

  def native3(topic: String): (java.util.concurrent.LinkedBlockingQueue[com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish], () => Unit) = {
    import com.hivemq.client.mqtt.MqttClient
    import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe
    val client   = MqttClient.builder().useMqttVersion3().identifier(uniq("native")).serverHost(Brokers.mosquitto.getHost).serverPort(Brokers.mosquittoPort).buildAsync()
    client.connect().get()
    val received = new java.util.concurrent.LinkedBlockingQueue[com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish]()
    client.subscribe(Mqtt3Subscribe.builder().topicFilter(topic).build(), (p: com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish) => { received.add(p); () }).get()
    (received, () => { client.disconnect(); () })
  }

  test("mqtt 5: a cloudevent travels as user properties, which any mqtt 5 client reads") {
    val topic             = uniq("binary")
    val (received, close) = native5(topic)
    try {
      val event           = Json.obj("specversion" -> "1.0", "id" -> "m-1", "source" -> "/sensors", "type" -> "com.acme.reading", "tenant" -> "acme",
        "datacontenttype" -> "application/json", "data" -> message(0, "binary"))
      val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(event), ctype = "application/cloudevents+json")
      assertEquals(status, 200, lines.toString)
      val publish         = Option(received.poll(10, java.util.concurrent.TimeUnit.SECONDS)).getOrElse(fail("nothing received"))
      val properties      = publish.getUserProperties.asList().asScala.map(p => p.getName.toString -> p.getValue.toString).toMap
      assertEquals(properties.get("id"), Some("m-1"))
      assertEquals(properties.get("specversion"), Some("1.0"))
      assertEquals(properties.get("tenant"), Some("acme"))
      assertEquals(publish.getContentType.toScala.map(_.toString), Some("application/json"))
      assertEquals(Json.parse(publish.getPayloadAsBytes), message(0, "binary"))
    } finally close()
  }

  test("mqtt 3.1.1: no properties, the whole event travels as json") {
    val v3                = Gateway.createConnection("mqtt", Json.obj("host" -> Brokers.mosquitto.getHost, "port" -> Brokers.mosquittoPort, "version" -> 3))
    val ingesting         = Gateway.brokerRoute[HttpEventsIngester](v3)
    val consuming         = Gateway.brokerRoute[HttpEventsConsumer](v3)
    val topic             = uniq("v3")
    val (received, close) = native3(topic)
    try {
      val (status, lines) = Gateway.post(ingesting, "/" + opts("topic" -> topic), Json.stringify(message(0, "v3")))
      assertEquals(status, 200, lines.toString)
      val publish         = Option(received.poll(10, java.util.concurrent.TimeUnit.SECONDS)).getOrElse(fail("nothing received"))
      val json            = Json.parse(publish.getPayloadAsBytes)
      assertEquals((json \ "specversion").as[String], "1.0")
      assertEquals((json \ "id").as[String], (lines.head \ "id").as[String])
      // and back through the gateway
      val run             = uniq("run")
      val events          = collect(Gateway.ndjson(consuming, "/" + opts("topic" -> topic)), take = 3)
      keepPublishing(events)(n => Gateway.post(ingesting, "/" + opts("topic" -> topic), Json.stringify(message(n, run))))
      assertConsecutive(numbers(await(events)), 3)
    } finally {
      close()
      Gateway.deleteConnection(v3)
    }
  }

  test("mqtt 5: a group is a shared subscription, its subscribers share the messages") {
    val topic = uniq("shared")
    val run   = uniq("run")
    val path  = "/" + opts("topic" -> topic, "group" -> uniq("group"))
    val one   = collect(Gateway.ndjson(consumer, path), take = 10, within = 8.seconds)
    val other = collect(Gateway.ndjson(consumer, path), take = 10, within = 8.seconds)
    keepPublishing(one.zip(other), every = 50.millis, atMost = 8.seconds)(n => publish(topic, n, run))
    val (a, b) = (numbers(await(one)), numbers(await(other)))
    assert(a.nonEmpty && b.nonEmpty, s"$a / $b")
    assertEquals(a.intersect(b), Seq.empty, s"$a / $b")
  }

  test("over websockets") {
    val ws       = Gateway.createConnection(
      "mqtt",
      Json.obj("host" -> Brokers.mosquittoWs.getHost, "port" -> Brokers.mosquittoWs.getMappedPort(9001).intValue, "transport" -> "websocket", "websocket_path" -> "/mqtt")
    )
    val ingesting = Gateway.brokerRoute[HttpEventsIngester](ws)
    val consuming = Gateway.brokerRoute[HttpEventsConsumer](ws)
    val topic     = uniq("ws")
    val run       = uniq("run")
    val events    = collect(Gateway.ndjson(consuming, "/" + opts("topic" -> topic)), take = 3)
    keepPublishing(events)(n => Gateway.post(ingesting, "/" + opts("topic" -> topic), Json.stringify(message(n, run))))
    assertConsecutive(numbers(await(events)), 3)
    Gateway.deleteConnection(ws)
  }
}
