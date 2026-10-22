package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{Range, RedisClient}
import org.apache.pekko.http.scaladsl.model.HttpResponse
import otoroshi.next.models.NgRoute
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** a redis connection in streams mode: a log, with groups, acks and replay */
class RedisStreamsSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.redis; () }

  def streamsConnection(streams: JsObject = Json.obj()): String =
    Gateway.createConnection("redis", Json.obj("uri" -> Brokers.redisUri, "mode" -> "streams", "streams" -> (Json.obj("default_stream" -> "default-stream") ++ streams)))

  lazy val connection: String = streamsConnection()
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)
  lazy val sse                = Gateway.brokerRoute[SseEventConsumer](connection)
  /** what a member of a group leaves unacked goes to another one after a second */
  lazy val claiming           = Gateway.brokerRoute[HttpEventsConsumer](streamsConnection(Json.obj("claim_min_idle_ms" -> 1000)))

  def publish(stream: String, n: Int, run: String): JsValue = {
    val (status, lines) = Gateway.post(ingester, "/" + opts("stream" -> stream), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "published").asOpt[Boolean], Some(true), lines.toString)
    lines.head
  }

  def entryId(published: JsValue): String = (published \ "details" \ "id").as[String]

  def read(route: NgRoute, stream: String, options: (String, Any)*)(take: Int): Seq[JsValue] =
    await(collect(Gateway.ndjson(route, "/" + opts((Seq("stream" -> stream) ++ options)*)), take = take))

  def refused(options: (String, Any)*): String = {
    val res: HttpResponse = await(Gateway.request(consumer, path = "/" + opts(options*)))
    val body              = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    body
  }

  /** what any redis client sees */
  def native[A](f: RedisCommands[String, String] => A): A = {
    val client = RedisClient.create(Brokers.redisUri)
    try {
      val connection = client.connect()
      try f(connection.sync())
      finally connection.close()
    } finally client.shutdown()
  }

  test("publishes entries, the attributes of the event in ce_ fields any redis client reads") {
    val stream    = uniq("publish")
    val published = publish(stream, 0, "native")
    assertEquals((published \ "details" \ "stream").as[String], stream)
    val entry     = native(_.xrange(stream, Range.create("-", "+"))).asScala.toSeq
    assertEquals(entry.map(_.getId), Seq(entryId(published)))
    val body      = entry.head.getBody.asScala
    assertEquals(body.get("ce_specversion"), Some("1.0"))
    assertEquals(body.get("ce_id"), (published \ "id").asOpt[String])
    assertEquals(body.get("content-type"), Some("application/json"))
    assertEquals(body.get("data").map(Json.parse), Some(message(0, "native")))
  }

  test("from the beginning: what the stream holds, in order, as it was published") {
    val stream = uniq("earliest")
    val run    = uniq("run")
    val ids    = (0 until 3).map(n => (publish(stream, n, run) \ "id").as[String])
    val events = read(consumer, stream, "from" -> "earliest")(3)
    assertEquals(numbers(events), Seq(0, 1, 2))
    assertEquals(events.map(e => (e \ "id").as[String]), ids)
    assertEquals(ext(events.head, "redisstream"), Some(stream))
    assert(ext(events.head, "redisid").exists(_.matches("\\d+-\\d+")), events.head.toString)
  }

  test("from now on: what the stream held before is not delivered") {
    val stream   = uniq("latest")
    val run      = uniq("run")
    (0 until 3).foreach(n => publish(stream, n, "before"))
    val received = collect(Gateway.ndjson(consumer, "/" + opts("stream" -> stream)), take = 3)
    keepPublishing(received)(n => publish(stream, n, run))
    val events   = await(received)
    assert(events.forall(e => (payload(e) \ "run").as[String] == run), events.toString)
    assertConsecutive(numbers(events), 3)
  }

  test("from an entry id, or from a date") {
    val stream = uniq("from")
    val run    = uniq("run")
    val ids    = (0 until 5).map { n =>
      // the date of an entry is in its id, to the millisecond: the next ones come later
      if (n == 3) Thread.sleep(20L)
      entryId(publish(stream, n, run))
    }
    assertEquals(numbers(read(consumer, stream, "from_id" -> ids(2))(3)), Seq(2, 3, 4))
    assertEquals(numbers(read(consumer, stream, "from_timestamp" -> ids(3).takeWhile(_ != '-'))(2)), Seq(3, 4))
    assert(refused("stream" -> stream, "offset_from" -> 2).contains("no offsets"))
    assert(refused("stream" -> stream, "from_id" -> "yesterday").contains("from_id is a stream entry id"))
  }

  test("a group shares the entries between its members") {
    val stream = uniq("shared")
    val group  = uniq("group")
    val run    = uniq("run")
    def member(): Future[Seq[JsValue]] =
      collect(Gateway.ndjson(consumer, "/" + opts("stream" -> stream, "group" -> group, "prefetch" -> 1)), take = 3, within = 30.seconds)
    val (a, b) = (member(), member())
    keepPublishing(Future.sequence(Seq(a, b)), every = 100.millis)(n => publish(stream, n, run))
    val (na, nb) = (numbers(await(a)), numbers(await(b)))
    assertEquals((na.size, nb.size), (3, 3), s"$na $nb")
    assertEquals(na.intersect(nb), Seq.empty, s"$na $nb")
  }

  Seq("at-most-once", "at-least-once").foreach { delivery =>
    test(s"$delivery: what a member read and did not hand over goes to another member") {
      val stream = uniq("claimed")
      val group  = uniq("group")
      val run    = uniq("run")
      (0 until 5).foreach(n => publish(stream, n, run))
      // the first one reads the five of them at once, and hands two over
      def member(take: Int): Seq[JsValue] = read(claiming, stream, "group" -> group, "from" -> "earliest", "delivery" -> delivery, "take" -> take)(take)
      assertEquals(numbers(member(2)), Seq(0, 1))
      val rest   = member(3)
      assertEquals(numbers(rest), Seq(2, 3, 4))
      assert(rest.forall(e => ext(e, "redisredelivered").contains("true")), rest.toString)
      eventually("nothing pending")(native(_.xpending(stream, group)).getCount == 0L)
      // the first one went away with entries pending: it stays, the second one is gone
      eventually("one consumer left")(native(_.xinfoConsumers(stream, group)).size == 1)
    }
  }

  test("a named consumer coming back gets what it had pending first") {
    val stream = uniq("named")
    val group  = uniq("group")
    val run    = uniq("run")
    (0 until 5).foreach(n => publish(stream, n, run))
    def member(take: Int): Seq[JsValue] =
      read(consumer, stream, "group" -> group, "consumer" -> "c1", "from" -> "earliest", "delivery" -> "at-least-once", "take" -> take)(take)
    assertEquals(numbers(member(2)), Seq(0, 1))
    val again  = member(3)
    assertEquals(numbers(again), Seq(2, 3, 4))
    assert(again.forall(e => ext(e, "redisredelivered").contains("true")), again.toString)
    assertEquals(again.flatMap(ext(_, "redisconsumer")).distinct, Seq("c1"))
  }

  test("a member gone with nothing pending leaves the group") {
    val stream = uniq("leaves")
    val group  = uniq("group")
    publish(stream, 0, uniq("run"))
    assertEquals(numbers(read(consumer, stream, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "take" -> 1)(1)), Seq(0))
    eventually("no consumer left")(native(_.xinfoConsumers(stream, group)).size == 0)
  }

  test("at least once needs a group") {
    assert(refused("stream" -> uniq("acks"), "delivery" -> "at-least-once").contains("needs a group"))
  }

  test("sse: a client coming back resumes right after the last entry it got") {
    val stream = uniq("sse")
    val run    = uniq("run")
    (0 until 5).foreach(n => publish(stream, n, run))
    val first  = await(collect(Gateway.sse(sse, "/" + opts("stream" -> stream, "from" -> "earliest", "take" -> 2)).filter(_.id.isDefined), take = 2))
    assertEquals(numbers(first.map(e => Json.parse(e.data))), Seq(0, 1))
    assert(first.last.id.exists(_.startsWith("redis:")), first.toString)
    val rest   = await(collect(Gateway.sse(sse, "/" + opts("stream" -> stream), Seq("Last-Event-ID" -> first.last.id.get)).filter(_.id.isDefined), take = 3))
    assertEquals(numbers(rest.map(e => Json.parse(e.data))), Seq(2, 3, 4))
  }

  test("an entry added by another producer is read as an event: its fields as json, or its data field") {
    val stream = uniq("foreign")
    val fields = new java.util.LinkedHashMap[String, String]()
    fields.put("user", "42")
    fields.put("amount", "10")
    native(_.xadd(stream, fields))
    native(_.xadd(stream, "data", "hello", "content-type", "text/plain"))
    val events = read(consumer, stream, "from" -> "earliest")(2)
    val json   = events.head
    assertEquals((json \ "type").as[String], "com.cloud-apim.event-brokers.message")
    assertEquals((json \ "source").as[String], s"/redis/$stream")
    assertEquals((json \ "datacontenttype").as[String], "application/json")
    assertEquals(payload(json), Json.obj("user" -> "42", "amount" -> "10"))
    assertEquals((json \ "id").as[String], s"$stream-${ext(json, "redisid").get}")
    assertEquals(((events(1) \ "datacontenttype").as[String], (events(1) \ "data").as[String]), ("text/plain", "hello"))
  }

  test("a stream with a max length is trimmed as entries are added") {
    val stream   = uniq("trimmed")
    val trimming = Gateway.brokerRoute[HttpEventsIngester](streamsConnection(Json.obj("max_length" -> 3, "exact_trimming" -> true)))
    (0 until 6).foreach(n => assertEquals(Gateway.post(trimming, "/" + opts("stream" -> stream), Json.stringify(message(n, "t")))._1, 200))
    assertEquals(native(_.xlen(stream)).longValue(), 3L)
    assertEquals(numbers(read(consumer, stream, "from" -> "earliest")(3)), Seq(3, 4, 5))
  }
}
