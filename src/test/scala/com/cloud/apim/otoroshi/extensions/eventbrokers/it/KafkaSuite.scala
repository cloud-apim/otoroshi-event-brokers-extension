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

  import Gateway.given

  override def startBroker(): Unit = { Brokers.kafka; () }

  lazy val connection: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers), "default_topic" -> "default-topic"))
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)
  lazy val sse                = Gateway.brokerRoute[SseEventConsumer](connection)
  lazy val websocket          = Gateway.brokerRoute[IngesterConsumerWebsocket](
    connection,
    Json.obj("in" -> (Json.obj("ref" -> connection) ++ Gateway.everythingAllowed), "out" -> (Json.obj("ref" -> connection) ++ Gateway.everythingAllowed))
  )

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
    assertEquals(received.flatMap(e => ext(e, "kafkatopic")).distinct, Seq(topic))
    assertEquals(received.flatMap(e => ext(e, "kafkaoffset")), Seq("0", "1", "2"))
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

  test("text bodies keep their content type, charset included") {
    val topic           = uniq("text")
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), "hello", ctype = "text/plain; charset=UTF-8")
    assertEquals(status, 200, lines.toString)
    val received        = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 1))
    assertEquals((received.head \ "datacontenttype").asOpt[String], Some("text/plain; charset=UTF-8"), received.toString)
    assertEquals((received.head \ "data").asOpt[String], Some("hello"))
  }

  test("consumes every partition of a topic") {
    val topic    = uniq("partitions")
    val run      = uniq("run")
    createTopic(topic, 3)
    (0 until 3).foreach(n => publish(topic, n, run, "partition" -> n))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "auto_offset_reset" -> "earliest")), take = 3, within = 20.seconds))
    assertEquals(numbers(received).sorted, Seq(0, 1, 2))
  }

  /** the offset the group would read next on partition 0 of the topic */
  def committed(group: String, topic: String): Long = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Brokers.kafka.getBootstrapServers)
    val admin = AdminClient.create(props)
    try {
      Option(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(new org.apache.kafka.common.TopicPartition(topic, 0)))
        .map(_.offset())
        .getOrElse(-1L)
    } finally admin.close()
  }

  // `take` on the gateway's side: the events that went out are the ones acked
  test("at least once in a group: only what was handed over is committed, the next subscriber gets the rest") {
    val topic = uniq("group")
    val run   = uniq("run")
    val group = uniq("group")
    (0 until 5).foreach(n => publish(topic, n, run))
    val first = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "take" -> 2)), take = 2))
    assertEquals(numbers(first), Seq(0, 1))
    eventually("the acked offsets committed")(committed(group, topic) == 2L)
    val rest  = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "take" -> 3)), take = 3))
    assertEquals(numbers(rest), Seq(2, 3, 4))
  }

  test("starts from a date") {
    val topic = uniq("from-date")
    val run   = uniq("run")
    (0 until 2).foreach(n => publish(topic, n, run))
    Thread.sleep(200L)
    val since = System.currentTimeMillis()
    Thread.sleep(200L)
    (2 until 4).foreach(n => publish(topic, n, run))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from_timestamp" -> since)), take = 2))
    assertEquals(numbers(received), Seq(2, 3))
  }

  test("starts from an offset") {
    val topic = uniq("from-offset")
    val run   = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "offset_from" -> 3)), take = 2))
    assertEquals(numbers(received), Seq(3, 4))
  }

  test("publishes a batch: one result per event, in order") {
    val topic           = uniq("batch")
    val run             = uniq("run")
    val events          = Json.obj("events" -> JsArray((0 until 3).map { n =>
      Json.obj("value" -> Json.obj("type" -> "json", "data" -> message(n, run)), "vendor_metadata" -> Json.obj("topic" -> topic))
    }))
    val (status, lines) = Gateway.post(ingester, "/", Json.stringify(events), ctype = "application/vnd.cloud-apim-event-broker+json")
    assertEquals(status, 200, lines.toString)
    assertEquals(lines.map(l => (l \ "details" \ "offset").as[Long]), Seq(0L, 1L, 2L))
    assertEquals(lines.map(l => (l \ "details" \ "topic").as[String]).distinct, Seq(topic))
  }

  test("a delivery that does not exist is refused") {
    val res = await(Gateway.request(consumer, path = "/" + opts("topic" -> uniq("bad"), "delivery" -> "exactly-once")))
    res.discardEntityBytes()
    assertEquals(res.status.intValue(), 400)
  }

  /** what a plain kafka consumer, outside of otoroshi, reads from the topic */
  def nativeRecords(topic: String, count: Int): Seq[org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]] = {
    import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Brokers.kafka.getBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, uniq("native"))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer].getName)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    try {
      consumer.subscribe(java.util.List.of(topic))
      val deadline = System.currentTimeMillis() + 20000L
      var records  = Seq.empty[org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]]]
      while (records.size < count && System.currentTimeMillis() < deadline) records = records ++ consumer.poll(java.time.Duration.ofMillis(200)).asScala
      records
    } finally consumer.close()
  }

  def header(record: org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]], name: String): Option[String] =
    Option(record.headers().lastHeader(name)).map(h => new String(h.value(), "UTF-8"))

  test("a cloudevent goes through as it is, in ce_ headers any kafka consumer reads") {
    val topic           = uniq("cloudevent")
    val event           = Json.obj(
      "specversion"     -> "1.0",
      "id"              -> uniq("id"),
      "source"          -> "/acme/orders",
      "type"            -> "com.acme.order.created",
      "subject"         -> "o-42",
      "partitionkey"    -> "o-42",
      "tenant"          -> "acme",
      "datacontenttype" -> "application/json",
      "data"            -> message(0, "ce")
    )
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(event), ctype = "application/cloudevents+json")
    assertEquals(status, 200, lines.toString)
    assertEquals((lines.head \ "id").as[String], (event \ "id").as[String])
    val received        = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 1)).head
    Seq("id", "source", "type", "subject", "partitionkey", "tenant", "datacontenttype").foreach(name => assertEquals(received \ name, event \ name, name))
    assertEquals(payload(received), message(0, "ce"))
    val record          = nativeRecords(topic, 1).head
    assertEquals(header(record, "ce_id"), (event \ "id").asOpt[String])
    assertEquals(header(record, "ce_type"), Some("com.acme.order.created"))
    assertEquals(header(record, "content-type"), Some("application/json"))
    assertEquals(new String(record.key(), "UTF-8"), "o-42")
    assertEquals(Json.parse(record.value()), message(0, "ce"))
  }

  test("http binary mode: the attributes in ce- headers, the body as data") {
    val topic           = uniq("binary")
    val (status, lines) = Gateway.post(
      ingester,
      "/" + opts("topic" -> topic),
      "hello",
      ctype = "text/plain; charset=UTF-8",
      headers = Seq("ce-id" -> "b-1", "ce-type" -> "com.acme.greeting", "ce-source" -> "/greeter", "ce-lang" -> "en")
    )
    assertEquals(status, 200, lines.toString)
    val received        = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 1)).head
    assertEquals((received \ "id").as[String], "b-1")
    assertEquals((received \ "type").as[String], "com.acme.greeting")
    assertEquals((received \ "source").as[String], "/greeter")
    assertEquals((received \ "lang").as[String], "en")
    assertEquals((received \ "data").as[String], "hello")
  }

  test("without attributes, the event gets an id, the route as source, and a time") {
    val topic           = uniq("defaults")
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(0, "d")))
    assertEquals(status, 200, lines.toString)
    val received        = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 1)).head
    assertEquals((received \ "id").as[String], (lines.head \ "id").as[String])
    assertEquals((received \ "source").as[String], s"/otoroshi/routes/${ingester.id}")
    assertEquals((received \ "type").as[String], "com.cloud-apim.event-brokers.event")
    assert((received \ "time").asOpt[String].isDefined, received.toString)
  }

  test("a batch of cloudevents: one result per event, with its id") {
    val topic           = uniq("ce-batch")
    val batch           = JsArray((0 until 3).map { n =>
      Json.obj("specversion" -> "1.0", "id" -> s"batch-$n", "source" -> "/s", "type" -> "t", "data" -> message(n, "batch"))
    })
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(batch), ctype = "application/cloudevents-batch+json")
    assertEquals(status, 200, lines.toString)
    assertEquals(lines.map(l => (l \ "id").as[String]), Seq("batch-0", "batch-1", "batch-2"))
    assertEquals(lines.map(l => (l \ "details" \ "offset").as[Long]), Seq(0L, 1L, 2L))
  }

  test("what is not a cloudevent is refused with a 400") {
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> uniq("invalid")), """{"specversion":"1.0","id":"1"}""", ctype = "application/cloudevents+json")
    assertEquals(status, 400, lines.toString)
    assert(lines.toString.contains("source is required"), lines.toString)
  }

  test("a message from another producer is read as a cloudevent: its position as id") {
    import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
    val topic    = uniq("foreign")
    val props    = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Brokers.kafka.getBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName)
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    try producer.send(new ProducerRecord(topic, Json.stringify(message(0, "foreign")).getBytes("UTF-8"))).get()
    finally producer.close()
    val received = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = 1)).head
    assertEquals((received \ "id").as[String], s"$topic-0-0")
    assertEquals((received \ "source").as[String], s"/kafka/$topic")
    assertEquals((received \ "type").as[String], "com.cloud-apim.event-brokers.message")
    assertEquals((received \ "datacontenttype").as[String], "application/json")
    assertEquals(payload(received), message(0, "foreign"))
  }

  test("raw: the data alone") {
    val topic = uniq("raw")
    val run   = uniq("run")
    (0 until 2).foreach(n => publish(topic, n, run))
    val lines = await(collect(Gateway.lines(consumer, "/" + opts("topic" -> topic, "from" -> "earliest", "format" -> "raw")).filter(_.nonEmpty), take = 2))
    assertEquals(lines.map(Json.parse), Seq(message(0, run), message(1, run)))
  }

  test("sse: the event is named after its type, and a client coming back resumes right after what it got") {
    val topic  = uniq("resume")
    val run    = uniq("run")
    (0 until 5).foreach(n => publish(topic, n, run))
    val first  = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic, "from" -> "earliest", "take" -> 2)).filter(_.id.isDefined), take = 2))
    assertEquals(numbers(first.map(e => Json.parse(e.data))), Seq(0, 1))
    assertEquals(first.flatMap(_.field("event")).distinct, Seq("com.cloud-apim.event-brokers.event"))
    val rest   = await(collect(Gateway.sse(sse, "/" + opts("topic" -> topic), Seq("Last-Event-ID" -> first.last.id.get)).filter(_.id.isDefined), take = 3))
    assertEquals(numbers(rest.map(e => Json.parse(e.data))), Seq(2, 3, 4))
  }

  test("sse: a heartbeat while nothing happens") {
    val beats = await(collect(Gateway.sse(sse, "/" + opts("topic" -> uniq("quiet"), "heartbeat" -> 200)), take = 2, within = 5.seconds))
    assertEquals(beats.size, 2, beats.toString)
    assert(beats.forall(_.lines.forall(_.startsWith(":"))), beats.toString)
  }

  test("websocket, acks=client: only what the client acked is committed, the rest comes back") {
    val topic   = uniq("ws-acks")
    val group   = uniq("group")
    val run     = uniq("run")
    (0 until 3).foreach(n => publish(topic, n, run))
    val path    = "/" + opts("topic" -> topic, "group" -> group, "from" -> "earliest", "delivery" -> "at-least-once", "acks" -> "client", "max_in_flight" -> 1)
    val first   = Gateway.websocketSession(websocket, path)
    val zero    = Json.parse(first.next().get)
    assertEquals(numbers(Seq(zero)), Seq(0))
    // one in flight: nothing else comes before the ack
    assertEquals(first.next(1.second), None)
    first.send(Json.stringify(Json.obj("ack" -> (zero \ "id").as[String])))
    val one     = Json.parse(first.next().get)
    assertEquals(numbers(Seq(one)), Seq(1))
    first.close()
    eventually("the acked offset committed")(committed(group, topic) == 1L)
    val second  = Gateway.websocketSession(websocket, path)
    assertEquals(numbers(Seq(Json.parse(second.next().get))), Seq(1))
    second.close()
  }

  test("websocket, publish_results: every publish is answered") {
    val topic   = uniq("ws-results")
    val route   = Gateway.brokerRoute[IngesterConsumerWebsocket](connection, Json.obj("in" -> (Json.obj("ref" -> connection) ++ Gateway.everythingAllowed)))
    val session = Gateway.websocketSession(route, "/" + opts("topic" -> topic, "publish_results" -> true, "content-type" -> "application/json"))
    session.send(Json.stringify(message(0, "r")))
    session.send(Json.stringify(Json.obj("specversion" -> "1.0", "id" -> "ws-1", "source" -> "/ws", "type" -> "t", "data" -> message(1, "r"))))
    val results = Seq(session.next(), session.next()).flatten.map(Json.parse)
    session.close()
    assertEquals(results.map(r => (r \ "published").as[Boolean]), Seq(true, true), results.toString)
    assertEquals((results(1) \ "id").as[String], "ws-1")
    assertEquals(results.map(r => (r \ "details" \ "offset").as[Long]), Seq(0L, 1L))
  }
}
