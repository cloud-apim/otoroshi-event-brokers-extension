package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

class RedisSuite extends BrokerSuite {

  import Gateway.given

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

  test("http consumer: what is published comes back, in order") {
    val channel  = uniq("http")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> channel)), take = 3, within = 15.seconds)
    keepPublishing(received, atMost = 15.seconds)(n => publish(channel, n, run))
    assertConsecutive(numbers(await(received)), 3)
  }

  /** how many subscribers redis counts on the channel */
  def subscribersOf(channel: String): Long = {
    val client = io.lettuce.core.RedisClient.create(Brokers.redisUri)
    try {
      val connection = client.connect()
      try Option(connection.sync().pubsubNumsub(channel).get(channel)).map(_.longValue()).getOrElse(0L)
      finally connection.close()
    } finally client.shutdown()
  }

  test("a subscriber of a channel gets nothing of another one") {
    val mine     = uniq("mine")
    val other    = uniq("other")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> mine)), take = 1, within = 20.seconds)
    eventually("subscribed")(subscribersOf(mine) == 1L)
    (0 until 5).foreach(n => publish(other, n, run))
    publish(mine, 42, run)
    val events   = await(received)
    assertEquals(numbers(events), Seq(42))
    assertEquals(ext(events.head, "redischannel"), Some(mine))
  }

  test("a pattern gets every channel it matches") {
    val prefix   = uniq("pattern")
    val run      = uniq("run")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> s"$prefix.*")), take = 2, within = 20.seconds)
    keepPublishing(received)(n => publish(s"$prefix.${if (n % 2 == 0) "a" else "b"}", n, run))
    val events   = await(received)
    assertEquals(events.size, 2, events.toString)
    assert(events.forall(e => ext(e, "redispattern").contains(s"$prefix.*")), events.toString)
    assert(events.forall(e => ext(e, "redischannel").exists(_.startsWith(s"$prefix."))), events.toString)
  }

  test("a subscriber that goes away lets go of its subscription") {
    val channel = uniq("gone")
    val run     = uniq("run")
    val one     = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> channel)), take = 1, within = 20.seconds)
    eventually("subscribed")(subscribersOf(channel) == 1L)
    keepPublishing(one)(n => publish(channel, n, run))
    await(one)
    eventually("unsubscribed")(subscribersOf(channel) == 0L)
  }

  test("at least once is refused: redis pub/sub cannot acknowledge") {
    val res  = await(Gateway.request(consumer, path = "/" + opts("channel" -> uniq("acks"), "delivery" -> "at-least-once")))
    val body = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    assert(body.contains("cannot acknowledge"), body)
  }

  test("a message published by someone else is read as a cloudevent") {
    val channel  = uniq("foreign")
    val received = collect(Gateway.ndjson(consumer, "/" + opts("channel" -> channel)), take = 1, within = 20.seconds)
    val client   = io.lettuce.core.RedisClient.create(Brokers.redisUri)
    val conn     = client.connect()
    try keepPublishing(received)(n => { conn.sync().publish(channel, Json.stringify(message(n, "foreign"))); () })
    finally { conn.close(); client.shutdown() }
    val event    = await(received).head
    assertEquals((event \ "type").as[String], "com.cloud-apim.event-brokers.message")
    assertEquals((event \ "source").as[String], s"/redis/$channel")
    assertEquals((event \ "datacontenttype").as[String], "application/json")
    assertEquals((payload(event) \ "run").as[String], "foreign")
  }
}
