package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.rabbitmq.client.ConnectionFactory
import play.api.libs.json.*
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String

/** trying a connection from the admin api: publishing a test event, peeking at what is there */
class TryoutSuite extends BrokerSuite {

  override def startBroker(): Unit = { Brokers.kafka; Brokers.rabbitmq; () }

  lazy val kafka: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers)))

  lazy val rabbitmq: String = Gateway.createConnection(
    "rabbitmq",
    Json.obj(
      "addresses" -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "username"  -> Brokers.rabbitmq.getAdminUsername,
      "password"  -> Brokers.rabbitmq.getAdminPassword
    )
  )

  def path(connection: String, action: String, query: (String, Any)*): String =
    s"/api/extensions/cloud-apim/event-brokers/connections/$connection/$action" + opts(query*).replace("x-ca-event-broker-opts-", "")

  test("publishes a test event, plain json or a cloudevent, and peeks at it") {
    val topic = uniq("tryout")
    val plain = await(Gateway.admin(path(kafka, "_publish", "topic" -> topic)).post(Json.stringify(message(0, "tryout"))))
    assertEquals(plain.status, 200, plain.body)
    assertEquals(((plain.json \ "results").as[Seq[JsValue]].head \ "published").as[Boolean], true)
    val event = Json.obj("specversion" -> "1.0", "id" -> uniq("id"), "source" -> "/tests", "type" -> "test.event", "data" -> message(1, "tryout"))
    // a cloudevent is told apart by what it holds
    val ce    = await(Gateway.admin(path(kafka, "_publish", "topic" -> topic)).post(Json.stringify(event)))
    assertEquals(ce.status, 200, ce.body)
    val peek  = await(Gateway.admin(path(kafka, "_peek", "topic" -> topic, "from" -> "earliest", "take" -> 2, "within" -> 10000)).get())
    assertEquals(peek.status, 200, peek.body)
    val found = (peek.json \ "events").as[Seq[JsValue]]
    assertEquals(numbers(found), Seq(0, 1))
    assertEquals((found(1) \ "type").as[String], "test.event")
  }

  test("a peek at a queue leaves what it saw in the queue") {
    val queue   = uniq("peeked")
    val factory = new ConnectionFactory()
    factory.setHost(Brokers.rabbitmq.getHost)
    factory.setPort(Brokers.rabbitmq.getAmqpPort)
    factory.setUsername(Brokers.rabbitmq.getAdminUsername)
    factory.setPassword(Brokers.rabbitmq.getAdminPassword)
    val amqp    = factory.newConnection()
    try {
      val channel = amqp.createChannel()
      channel.queueDeclare(queue, false, false, false, null)
      (0 until 2).foreach(n => assertEquals(await(Gateway.admin(path(rabbitmq, "_publish", "queue" -> queue)).post(Json.stringify(message(n, "q")))).status, 200))
      (0 until 2).foreach { _ =>
        val peek = await(Gateway.admin(path(rabbitmq, "_peek", "queue" -> queue, "take" -> 2, "within" -> 5000)).get())
        assertEquals(peek.status, 200, peek.body)
        assertEquals(numbers((peek.json \ "events").as[Seq[JsValue]]), Seq(0, 1))
        assertEquals((peek.json \ "acked").as[Boolean], false)
      }
      eventually("still in the queue")(channel.messageCount(queue) == 2L)
    } finally amqp.close()
  }

  test("an unknown connection is a 404, a peek the broker cannot serve a 400") {
    assertEquals(await(Gateway.admin(path("nope", "_peek", "topic" -> "t")).get()).status, 404)
    val res = await(Gateway.admin(path(rabbitmq, "_peek", "queue" -> uniq("q"), "group" -> "g")).get())
    assertEquals(res.status, 400, res.body)
  }
}
