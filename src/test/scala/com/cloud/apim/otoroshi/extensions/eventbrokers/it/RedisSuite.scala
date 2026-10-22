package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class RedisSuite extends BrokerSuite {

  override def startBroker(): Unit = { Brokers.redis; () }

  lazy val connection: String = Gateway.createConnection("redis", Json.obj("uri" -> Brokers.redisUri, "default_channel" -> "default-channel"))
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)

  def publish(channel: String, n: Int, run: String): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts("channel" -> channel), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    lines.head
  }

  test("publishes on a channel") {
    val res = publish(uniq("publish"), 0, uniq("run"))
    assertEquals((res \ "published").asOpt[Boolean], Some(true), res.toString)
  }

  // `subscribe` returns a lazy Mono that nobody subscribes to: the SUBSCRIBE command is never sent
  test("http consumer: what is published comes back, in order".fail) {
    val channel  = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> channel)), take = 3, within = 15.seconds)
    keepPublishing(received, atMost = 15.seconds)(n => publish(channel, n, run))
    assertConsecutive(numbers(await(received)), 3)
  }
}
