package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

/** what goes through the connections and the routes of a node, and how far behind the groups are */
class ObservabilitySuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.kafka; Brokers.redis; Brokers.rabbitmq; () }

  lazy val kafka: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers)))
  lazy val ingester      = Gateway.brokerRoute[HttpEventsIngester](kafka)
  lazy val consumer      = Gateway.brokerRoute[HttpEventsConsumer](kafka)
  lazy val sse           = Gateway.brokerRoute[SseEventConsumer](kafka)

  def traffic(): JsValue = await(Gateway.admin("/api/extensions/cloud-apim/event-brokers/_traffic").get()).json

  def counter(scope: String, id: String, name: String): Long = (traffic() \ scope \ id \ name).asOpt[Long].getOrElse(0L)

  def publish(topic: String, n: Int, run: String): Unit =
    assertEquals(Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(n, run)))._1, 200)

  test("the traffic of a connection and of its routes: published, consumed, acked") {
    val topic     = uniq("traffic")
    val run       = uniq("run")
    val published = counter("connections", kafka, "published")
    (0 until 3).foreach(n => publish(topic, n, run))
    assertEquals(numbers(await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 3))), Seq(0, 1, 2))
    eventually("counted") {
      counter("connections", kafka, "published") >= published + 3 && counter("routes", ingester.id, "published") >= 3 &&
      counter("routes", consumer.id, "consumed") >= 3 && counter("routes", consumer.id, "acked") >= 3
    }
    // in the status of the connection too
    val status = await(Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/connections/$kafka/_status").get()).json
    assert((status \ "traffic" \ "published").as[Long] >= 3L, status.toString)
    assert((status \ "traffic" \ "publish_ms" \ "max").asOpt[Long].isDefined, status.toString)
  }

  test("the subscriptions open on a route, while they stream") {
    val topic  = uniq("open")
    val before = counter("routes", sse.id, "subscriptions")
    val switch = Gateway.sse(sse, "/" + opts("topic" -> topic, "heartbeat" -> 200)).viaMat(org.apache.pekko.stream.KillSwitches.single)(org.apache.pekko.stream.scaladsl.Keep.right)
      .to(org.apache.pekko.stream.scaladsl.Sink.ignore).run()
    eventually("open")(counter("routes", sse.id, "subscriptions") == before + 1)
    switch.shutdown()
    eventually("closed")(counter("routes", sse.id, "subscriptions") == before)
  }

  test("a publish the broker refuses is counted as failed") {
    val rabbit = Gateway.createConnection("rabbitmq", Json.obj(
      "addresses" -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "username"  -> Brokers.rabbitmq.getAdminUsername,
      "password"  -> Brokers.rabbitmq.getAdminPassword
    ))
    val route  = Gateway.brokerRoute[HttpEventsIngester](rabbit)
    val (_, lines) = Gateway.post(route, "/" + opts("queue" -> uniq("nowhere")), Json.stringify(message(0, "lost")))
    assertEquals((lines.head \ "published").as[Boolean], false)
    eventually("counted")(counter("connections", rabbit, "publish_failed") == 1L && counter("routes", route.id, "publish_failed") == 1L)
  }

  test("otoroshi's metrics get the traffic too") {
    val env = Gateway.instance.env
    if (env.metricsEnabled) {
      publish(uniq("metrics"), 0, uniq("run"))
      eventually("exported")(env.metrics.prometheusExport().contains("event_brokers_connections"))
    }
  }

  test("how far behind a kafka group is") {
    val topic = uniq("lagging")
    val group = uniq("group")
    val run   = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    assertEquals(numbers(await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "take" -> 2)), take = 2))), Seq(0, 1))
    eventually("committed") {
      val lag = await(Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/connections/$kafka/_lag?group=$group&topic=$topic").get())
      lag.status == 200 && (lag.json \ "lag").asOpt[Long].contains(3L) && ((lag.json \ "partitions").as[Seq[JsValue]].head \ "committed").asOpt[Long].contains(2L)
    }
  }

  test("how far behind a redis streams group is, and a broker that does not tell") {
    val redis   = Gateway.createConnection("redis", Json.obj("uri" -> Brokers.redisUri, "mode" -> "streams"))
    val in      = Gateway.brokerRoute[HttpEventsIngester](redis)
    val out     = Gateway.brokerRoute[HttpEventsConsumer](redis)
    val stream  = uniq("lagging")
    val group   = uniq("group")
    (0 until 5).foreach(n => assertEquals(Gateway.post(in, "/" + opts("stream" -> stream), Json.stringify(message(n, "lag")))._1, 200))
    await(collect(Gateway.ndjson(out, "/" + opts("stream" -> stream, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "prefetch" -> 2, "take" -> 2)), take = 2))
    eventually("acked") {
      val lag = await(Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/connections/$redis/_lag?group=$group&topic=$stream").get())
      lag.status == 200 && (lag.json \ "lag").asOpt[Long].contains(3L) && ((lag.json \ "streams").as[Seq[JsValue]].head \ "pending").asOpt[Long].contains(0L)
    }
    val rabbit  = Gateway.createConnection("rabbitmq", Json.obj(
      "addresses" -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "username"  -> Brokers.rabbitmq.getAdminUsername,
      "password"  -> Brokers.rabbitmq.getAdminPassword
    ))
    assertEquals(await(Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/connections/$rabbit/_lag?group=g").get()).status, 501)
  }
}
