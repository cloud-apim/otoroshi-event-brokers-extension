package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class RabbitMQSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.rabbitmq; () }

  lazy val connection: String = Gateway.createConnection(
    "rabbitmq",
    Json.obj(
      "addresses"     -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "virtual_host"  -> "/",
      "username"      -> Brokers.rabbitmq.getAdminUsername,
      "password"      -> Brokers.rabbitmq.getAdminPassword,
      "default_queue" -> "default-queue"
    )
  )
  lazy val ingester = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection)
  lazy val sse      = Gateway.brokerRoute[SseEventConsumer](connection)

  /** a channel of our own, to set up queues and look at them behind the gateway's back */
  lazy val amqp: Connection = {
    val factory = new ConnectionFactory()
    factory.setHost(Brokers.rabbitmq.getHost)
    factory.setPort(Brokers.rabbitmq.getAmqpPort)
    factory.setUsername(Brokers.rabbitmq.getAdminUsername)
    factory.setPassword(Brokers.rabbitmq.getAdminPassword)
    factory.newConnection()
  }
  lazy val channel: Channel = amqp.createChannel()

  override def afterAll(): Unit = {
    super.afterAll()
    if (!munitIgnore) amqp.close()
  }

  /** outside of dev mode the extension declares nothing: the queue has to exist */
  def queue(): String = {
    val name = uniq("queue")
    channel.queueDeclare(name, false, false, false, null)
    name
  }

  def publish(queue: String, n: Int, run: String): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts("queue" -> queue), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  test("publishes to a queue") {
    val q   = queue()
    val run = uniq("run")
    val res = publish(q, 0, run)
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
    eventually("the message in the queue")(channel.messageCount(q) == 1L)
    val got = channel.basicGet(q, true)
    assertEquals(Json.parse(got.getBody), message(0, run))
  }

  test("http consumer: what is published comes back, in order") {
    val q        = queue()
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(q, n, run))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("queue" -> q)), take = 3))
    assertEquals(numbers(received), Seq(0, 1, 2))
    assertEquals(received.flatMap(e => ext(e, "rabbitmqroutingkey")).distinct, Seq(q))
  }

  test("sse consumer: one event per message") {
    val q        = queue()
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(q, n, run))
    val received = await(collect(Gateway.sse(sse, "/" + opts("queue" -> q)), take = 3))
    assertEquals(numbers(received.map(e => Json.parse(e.data))), Seq(0, 1, 2))
  }

  test("lets go of the queue when the http client goes away") {
    val q   = queue()
    val run = uniq("run")
    publish(q, 0, run)
    val got = await(collect(Gateway.ndjson(consumer, "/" + opts("queue" -> q)), take = 1))
    assertEquals(numbers(got), Seq(0))
    eventually("no consumer left on the queue", 10.seconds)(channel.consumerCount(q) == 0L)
  }

  test("a publish no queue gets is reported, not lost in silence") {
    val (status, lines) = Gateway.post(ingester, "/" + opts("queue" -> uniq("nowhere")), Json.stringify(message(0, uniq("run"))))
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "published").asOpt[Boolean], Some(false), lines.toString)
    assert((lines.head \ "error_description").as[String].contains("no queue got the message"), lines.toString)
  }

  test("consuming a queue that does not exist answers 502") {
    val res = await(Gateway.request(consumer, path = "/" + opts("queue" -> uniq("nowhere"))))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 502, body)
    assert(body.contains("consume_error"), body)
  }

  // `take` on the gateway's side: the events that went out are the ones acked
  test("at least once: what was not acked goes back to the queue, for the next subscriber") {
    val q     = queue()
    val run   = uniq("run")
    (0 until 3).foreach(n => publish(q, n, run))
    val first = await(collect(Gateway.ndjson(consumer, "/" + opts("queue" -> q, "delivery" -> "at-least-once", "take" -> 1)), take = 1))
    assertEquals(numbers(first), Seq(0))
    eventually("no consumer left on the queue")(channel.consumerCount(q) == 0L)
    val rest  = await(collect(Gateway.ndjson(consumer, "/" + opts("queue" -> q, "delivery" -> "at-least-once", "take" -> 2)), take = 2))
    assertEquals(numbers(rest).sorted, Seq(1, 2))
    assert(rest.forall(e => ext(e, "rabbitmqredelivered").contains("true")), rest.toString)
  }

  test("through an exchange: every subscriber gets every message") {
    val exchange = uniq("exchange")
    channel.exchangeDeclare(exchange, "direct", false, true, null)
    val run      = uniq("run")
    val path     = "/" + opts("exchange" -> exchange, "routing_key" -> "orders")
    val one      = collect(Gateway.ndjson(consumer, path), take = 3)
    val other    = collect(Gateway.ndjson(consumer, path), take = 3)
    // the queues of the subscribers are bound once their response starts: keep publishing until both have their share
    keepPublishing(one.zip(other))(n => Gateway.post(ingester, path, Json.stringify(message(n, run))))
    assertConsecutive(numbers(await(one)), 3)
    assertConsecutive(numbers(await(other)), 3)
  }

  /** a connection of its own, with a topology */
  def withTopology(topology: JsObject, waitConnected: Boolean = true): String = Gateway.createConnection(
    "rabbitmq",
    Json.obj(
      "addresses" -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "username"  -> Brokers.rabbitmq.getAdminUsername,
      "password"  -> Brokers.rabbitmq.getAdminPassword,
      "topology"  -> topology
    ),
    waitConnected
  )

  test("the topology of the connection is declared as it starts: exchanges, queues, bindings") {
    val exchange   = uniq("orders")
    val billing    = uniq("billing")
    val connection = withTopology(Json.obj(
      "exchanges" -> Json.arr(Json.obj("name" -> exchange, "type" -> "topic")),
      "queues"    -> Json.arr(Json.obj("name" -> billing, "type" -> "quorum", "arguments" -> Json.obj("x-max-length" -> 1000))),
      "bindings"  -> Json.arr(Json.obj("exchange" -> exchange, "queue" -> billing, "routing_key" -> "order.*"))
    ))
    val in         = Gateway.brokerRoute[HttpEventsIngester](connection)
    val out        = Gateway.brokerRoute[HttpEventsConsumer](connection)
    val run        = uniq("run")
    (0 until 3).foreach { n =>
      val (status, lines) = Gateway.post(in, "/" + opts("exchange" -> exchange, "routing_key" -> (if (n == 1) "invoice.sent" else "order.created")), Json.stringify(message(n, run)))
      assertEquals(status, 200, lines.toString)
    }
    // what the binding lets through, in a quorum queue
    assertEquals(numbers(await(collect(Gateway.ndjson(out, "/" + opts("queue" -> billing)), take = 2))), Seq(0, 2))
    assertEquals(channel.queueDeclarePassive(billing).getMessageCount, 0)
  }

  test("a declaration the broker refuses keeps the connection unavailable, saying why") {
    val name  = queue()
    // a classic queue cannot be declared again as a quorum one
    val id    = withTopology(Json.obj("queues" -> Json.arr(Json.obj("name" -> name, "type" -> "quorum"))), waitConnected = false)
    eventually("unavailable")((Gateway.connectionStatus(id).json \ "state").asOpt[String].contains("unavailable"))
    val error = (Gateway.connectionStatus(id).json \ "error").as[String]
    assert(error.contains(s"could not declare the queue '$name'") && error.contains("PRECONDITION_FAILED"), error)
  }

  test("a stream replays: from the first message, an offset, a date, and an sse client resumes right after what it got") {
    val stream     = uniq("stream")
    val connection = withTopology(Json.obj("queues" -> Json.arr(Json.obj("name" -> stream, "type" -> "stream"))))
    val in         = Gateway.brokerRoute[HttpEventsIngester](connection)
    val out        = Gateway.brokerRoute[HttpEventsConsumer](connection)
    val events     = Gateway.brokerRoute[SseEventConsumer](connection)
    val run        = uniq("run")
    (0 until 5).foreach { n =>
      if (n == 3) Thread.sleep(1100L)
      assertEquals(Gateway.post(in, "/" + opts("queue" -> stream), Json.stringify(message(n, run)))._1, 200)
    }
    val all        = await(collect(Gateway.ndjson(out, "/" + opts("queue" -> stream, "from" -> "earliest")), take = 5))
    assertEquals(numbers(all), Seq(0, 1, 2, 3, 4))
    val offsets    = all.map(e => ext(e, "rabbitmqstreamoffset").get.toLong)
    assertEquals(numbers(await(collect(Gateway.ndjson(out, "/" + opts("queue" -> stream, "offset_from" -> offsets(2))), take = 3))), Seq(2, 3, 4))
    // a stream keeps the time of its chunks, to the second
    val since      = (all(3) \ "time").as[String]
    val fromDate   = numbers(await(collect(Gateway.ndjson(out, "/" + opts("queue" -> stream, "from_timestamp" -> java.time.Instant.parse(since).toEpochMilli)), take = 5, within = 5.seconds)))
    assert(fromDate.nonEmpty && fromDate.last == 4 && fromDate.contains(3), fromDate.toString)
    val first      = await(collect(Gateway.sse(events, "/" + opts("queue" -> stream, "from" -> "earliest", "take" -> 2)).filter(_.id.isDefined), take = 2))
    assert(first.last.id.exists(_.startsWith("rabbitmq:")), first.toString)
    val rest       = await(collect(Gateway.sse(events, "/" + opts("queue" -> stream), Seq("Last-Event-ID" -> first.last.id.get)).filter(_.id.isDefined), take = 3))
    assertEquals(numbers(rest.map(e => Json.parse(e.data))), Seq(2, 3, 4))
  }

  test("a queue that is not a stream does not replay") {
    val res  = await(Gateway.request(consumer, path = "/" + opts("queue" -> queue(), "from" -> "earliest")))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    assert(body.contains("only a rabbitmq stream replays"), body)
  }

  test("a group is refused: the consumers of a queue already share it") {
    val res = await(Gateway.request(consumer, path = "/" + opts("queue" -> queue(), "group" -> "g")))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    assert(body.contains("no consumer groups"), body)
  }

  test("the cloudevent attributes go in cloudEvents: application properties, the content type in the amqp one") {
    val q   = queue()
    val res = publish(q, 0, uniq("run"))
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
    eventually("the message in the queue")(channel.messageCount(q) == 1L)
    val got = channel.basicGet(q, true)
    assertEquals(got.getProps.getContentType, "application/json")
    assertEquals(Option(got.getProps.getHeaders.get("cloudEvents:id")).map(_.toString), (res \ "id").asOpt[String])
    assertEquals(Option(got.getProps.getHeaders.get("cloudEvents:specversion")).map(_.toString), Some("1.0"))
  }
}
