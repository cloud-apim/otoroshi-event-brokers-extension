package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class RabbitMQSuite extends BrokerSuite {

  override def startBroker(): Unit = { Brokers.rabbitmq; () }

  lazy val connection: String = Gateway.createConnection(
    "rabbitmq",
    Json.obj(
      "servers"      -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "virtual_host" -> "/",
      "username"     -> Brokers.rabbitmq.getAdminUsername,
      "password"     -> Brokers.rabbitmq.getAdminPassword,
      "queue"        -> "default-queue"
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

  override def afterAll(): Unit = if (!munitIgnore) amqp.close()

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
    assertEquals(received.map(e => (e \ "vendor_metadata" \ "routing_key").as[String]).distinct, Seq(q))
  }

  test("sse consumer: one event per message") {
    val q        = queue()
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(q, n, run))
    val received = await(collect(Gateway.sse(sse, "/" + opts("queue" -> q)), take = 3))
    assertEquals(numbers(received.map(e => Json.parse(e.data))), Seq(0, 1, 2))
  }

  // the consumer is registered with auto-ack and never cancelled: once the http client is gone it
  // keeps taking its share of the queue and drops it on the floor
  test("lets go of the queue when the http client goes away".fail) {
    val q   = queue()
    val run = uniq("run")
    publish(q, 0, run)
    val got = await(collect(Gateway.ndjson(consumer, "/" + opts("queue" -> q)), take = 1))
    assertEquals(numbers(got), Seq(0))
    eventually("no consumer left on the queue", 10.seconds)(channel.consumerCount(q) == 0L)
  }
}
