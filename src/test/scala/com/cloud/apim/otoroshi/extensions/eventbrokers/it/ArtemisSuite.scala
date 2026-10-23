package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import jakarta.jms.{BytesMessage, Session}
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.pekko.http.scaladsl.model.HttpResponse
import otoroshi.next.models.NgRoute
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.Future
import scala.concurrent.duration.*

class ArtemisSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.artemis; () }

  def artemisConnection(more: JsObject = Json.obj()): String =
    Gateway.createConnection("artemis", Json.obj("url" -> Brokers.artemisUrl, "username" -> "artemis", "password" -> "artemis", "default_address" -> "events") ++ more)

  lazy val connection: String = artemisConnection()
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)

  def publish(address: String, n: Int, run: String, options: (String, Any)*): JsValue = publishWith(ingester, address, n, run, options*)

  def publishWith(route: NgRoute, address: String, n: Int, run: String, options: (String, Any)*): JsValue = {
    val (status, lines) = Gateway.post(route, "/" + opts((Seq("address" -> address) ++ options)*), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "published").asOpt[Boolean], Some(true), lines.toString)
    lines.head
  }

  def subscription(address: String, options: (String, Any)*): String = "/" + opts((Seq("address" -> address) ++ options)*)

  def read(address: String, options: (String, Any)*)(take: Int, within: FiniteDuration = 20.seconds): Seq[JsValue] =
    await(collect(Gateway.ndjson(consumer, subscription(address, options*)), take = take, within = within))

  def refused(options: (String, Any)*): String = {
    val res: HttpResponse = await(Gateway.request(consumer, path = "/" + opts(options*)))
    val body              = await(res.entity.toStrict(10.seconds)).data.utf8String
    assertEquals(res.status.intValue(), 400, body)
    body
  }

  /** a jms connection, as any jms application would open one */
  def jms[A](f: Session => A): A = {
    val factory    = new ActiveMQConnectionFactory(Brokers.artemisUrl, "artemis", "artemis")
    val connection = factory.createConnection()
    try {
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      f(session)
    } finally {
      connection.close()
      factory.close()
    }
  }

  test("publishes to an address, and says what the broker did with each event") {
    val res = publish(uniq("publish"), 0, uniq("run"))
    assertEquals((res \ "details" \ "routing_type").as[String], "multicast")
    assert((res \ "details" \ "message_id").asOpt[String].exists(_.nonEmpty), res.toString)
  }

  test("multicast: every subscription gets every message") {
    val address  = uniq("multicast")
    val run      = uniq("run")
    val (a, b)   = (collect(Gateway.ndjson(consumer, subscription(address)), take = 3), collect(Gateway.ndjson(consumer, subscription(address)), take = 3))
    keepPublishing(Future.sequence(Seq(a, b)))(n => publish(address, n, run))
    val (na, nb) = (numbers(await(a)), numbers(await(b)))
    assertConsecutive(na, 3)
    assertConsecutive(nb, 3)
    assertEquals(ext(await(a).head, "artemisaddress"), Some(address))
  }

  test("anycast: the queue keeps what is published until it is read") {
    val queue = uniq("anycast")
    val run   = uniq("run")
    (0 until 3).foreach(n => publish(queue, n, run, "routing_type" -> "anycast"))
    val events = read(queue, "routing_type" -> "anycast")(3)
    assertEquals(numbers(events), Seq(0, 1, 2))
    assertEquals(ext(events.head, "artemisroutingtype"), Some("anycast"))
    assertEquals(ext(events.head, "artemisqueue"), Some(queue))
  }

  test("a group is a durable subscription: it keeps what is published while none of its members is there") {
    val address = uniq("durable")
    val group   = uniq("group")
    val run     = uniq("run")
    // the first member makes the queue of the group, and goes
    assertEquals(read(address, "group" -> group, "duration" -> 500)(1, within = 10.seconds), Seq.empty)
    (0 until 3).foreach(n => publish(address, n, run))
    assertEquals(numbers(read(address, "group" -> group)(3)), Seq(0, 1, 2))
  }

  test("the members of a group share its messages") {
    val address  = uniq("shared")
    val group    = uniq("group")
    val run      = uniq("run")
    def member(): Future[Seq[JsValue]] = collect(Gateway.ndjson(consumer, subscription(address, "group" -> group, "prefetch" -> 1)), take = 3, within = 30.seconds)
    val (a, b)   = (member(), member())
    keepPublishing(Future.sequence(Seq(a, b)), every = 100.millis)(n => publish(address, n, run))
    val (na, nb) = (numbers(await(a)), numbers(await(b)))
    assertEquals((na.size, nb.size), (3, 3), s"$na $nb")
    assertEquals(na.intersect(nb), Seq.empty, s"$na $nb")
  }

  Seq("at-most-once", "at-least-once").foreach { delivery =>
    test(s"$delivery: what a consumer did not hand over goes back to the queue, for the next one, as never delivered") {
      val queue = uniq("back")
      val run   = uniq("run")
      (0 until 5).foreach(n => publish(queue, n, run, "routing_type" -> "anycast"))
      // the first one gets the five of them at once, and hands two over
      def next(take: Int): Seq[JsValue] = read(queue, "routing_type" -> "anycast", "delivery" -> delivery, "take" -> take)(take)
      assertEquals(numbers(next(2)), Seq(0, 1))
      val rest  = next(3)
      assertEquals(numbers(rest), Seq(2, 3, 4))
      // a consumer closing gives back what it did not ack as not delivered (only a consumer failing counts
      // its messages as delivered): what never reached a client does not get closer to the dead letters
      assert(rest.forall(e => ext(e, "artemisdeliverycount").contains("1")), rest.toString)
    }
  }

  test("what a subscription cannot do is refused") {
    assert(refused("address" -> uniq("x"), "delivery" -> "at-least-once").contains("needs a group"))
    assert(refused("address" -> uniq("x"), "routing_type" -> "anycast", "group" -> "g").contains("already share it"))
    assert(refused("address" -> uniq("x"), "from" -> "earliest").contains("only delivers what is published from now on"))
    assert(refused("address" -> uniq("x"), "routing_type" -> "broadcast").contains("routing_type is multicast or anycast"))
  }

  test("the key of an event is the group id of its message") {
    val queue = uniq("keyed")
    publish(queue, 0, uniq("run"), "routing_type" -> "anycast", "key" -> "customer-42")
    val event = read(queue, "routing_type" -> "anycast")(1).head
    assertEquals(ext(event, "artemisgroupid"), Some("customer-42"))
    assertEquals(ext(event, "partitionkey"), Some("customer-42"))
  }

  test("a selector picks what a subscription reads") {
    val queue = uniq("selected")
    val run   = uniq("run")
    (0 until 4).foreach(n => publish(queue, n, run, "routing_type" -> "anycast", "type" -> (if (n % 2 == 0) "even" else "odd")))
    val odd   = read(queue, "routing_type" -> "anycast", "selector" -> "cloudEvents_type = 'odd'")(2)
    assertEquals(numbers(odd), Seq(1, 3))
    // the others are still there
    assertEquals(numbers(read(queue, "routing_type" -> "anycast")(2)), Seq(0, 2))
  }

  test("a jms application reads what the gateway publishes") {
    val address = uniq("to-jms")
    jms { session =>
      val subscriber = session.createConsumer(session.createTopic(address))
      val published  = publish(address, 7, "jms")
      val received   = subscriber.receive(10000L).asInstanceOf[BytesMessage]
      assert(received != null, "nothing received")
      val body       = new Array[Byte](received.getBodyLength.toInt)
      received.readBytes(body)
      assertEquals(Json.parse(body), message(7, "jms"))
      assertEquals(received.getStringProperty("cloudEvents_id"), (published \ "id").as[String])
      assertEquals(received.getStringProperty("cloudEvents_specversion"), "1.0")
      assertEquals(received.getStringProperty("cloudEvents_datacontenttype"), "application/json")
      assertEquals(received.getJMSMessageID, s"ID:${(published \ "details" \ "message_id").as[String]}")
    }
  }

  test("what a jms application sends is read as an event") {
    val queue = uniq("from-jms")
    jms { session =>
      session.createProducer(session.createQueue(queue)).send(session.createTextMessage("hello"))
    }
    val event = read(queue, "routing_type" -> "anycast")(1).head
    assertEquals((event \ "type").as[String], "com.cloud-apim.event-brokers.message")
    assertEquals((event \ "source").as[String], s"/artemis/$queue")
    assertEquals(((event \ "datacontenttype").as[String], (event \ "data").as[String]), ("text/plain", "hello"))
  }

  test("with duplicate detection, an event published twice is delivered once") {
    val route = Gateway.brokerRoute[HttpEventsIngester](artemisConnection(Json.obj("duplicate_detection" -> true)))
    val queue = uniq("duplicates")
    val event = Json.stringify(Json.obj("specversion" -> "1.0", "id" -> uniq("id"), "source" -> "/tests", "type" -> "t", "data" -> message(0, "dup")))
    (0 until 2).foreach { _ =>
      val (status, lines) = Gateway.post(route, "/" + opts("address" -> queue, "routing_type" -> "anycast"), event, ctype = "application/cloudevents+json")
      assertEquals(status, 200, lines.toString)
    }
    assertEquals(read(queue, "routing_type" -> "anycast")(2, within = 3.seconds).size, 1)
  }

  test("a delivery can be delayed") {
    val queue    = uniq("delayed")
    val start    = System.currentTimeMillis()
    publish(queue, 0, uniq("run"), "routing_type" -> "anycast", "delay" -> 1500)
    assertEquals(read(queue, "routing_type" -> "anycast")(1).size, 1)
    assert(System.currentTimeMillis() - start >= 1400L, s"delivered after ${System.currentTimeMillis() - start} ms")
  }

  test("with the wrong credentials the connection is unavailable, and says why") {
    val id = Gateway.createConnection("artemis", Json.obj("url" -> Brokers.artemisUrl, "username" -> "artemis", "password" -> "nope"), waitConnected = false)
    eventually("unavailable")((Gateway.connectionStatus(id).json \ "state").asOpt[String].contains("unavailable"))
    val error = (Gateway.connectionStatus(id).json \ "error").as[String]
    assert(error.contains("AMQ229031") || error.toLowerCase.contains("validate user"), error)
  }
}
