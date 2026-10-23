package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersWorkflows
import otoroshi.events.{CustomDataExporterContext, ExportResult}
import otoroshi.next.workflow.WorkflowFunction
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

/** the data exporter, the workflow functions and request reply */
class IntegrationsSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.kafka; Brokers.rabbitmq; () }

  lazy val kafka: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers)))
  lazy val ingester      = Gateway.brokerRoute[HttpEventsIngester](kafka)
  lazy val consumer      = Gateway.brokerRoute[HttpEventsConsumer](kafka)

  def read(topic: String, take: Int): Seq[JsValue] = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic, "from" -> "earliest")), take = take))

  def otoroshiEvent(n: Int): JsObject = Json.obj(
    "@id"        -> s"event-$n",
    "@timestamp" -> 1792663171364L,
    "@type"      -> "GatewayEvent",
    "@product"   -> "otoroshi",
    "@env"       -> "prod",
    "@serviceId" -> "route-1",
    "n"          -> n,
    "run"        -> "export"
  )

  test("the data exporter: otoroshi events as cloudevents, the route as key") {
    implicit val env: otoroshi.env.Env = Gateway.instance.env
    val topic    = uniq("exported")
    val exporter = new EventBrokersDataExporter()
    val result   = await(exporter.send((0 until 2).map(otoroshiEvent), CustomDataExporterContext(Json.obj("connection" -> kafka, "options" -> Json.obj("topic" -> topic)), null)))
    assertEquals(result, ExportResult.ExportResultSuccess)
    val events   = read(topic, 2)
    assertEquals(events.map(e => (e \ "id").as[String]), Seq("event-0", "event-1"))
    assertEquals((events.head \ "type").as[String], "io.otoroshi.GatewayEvent")
    assertEquals((events.head \ "source").as[String], "/otoroshi/prod")
    assertEquals((events.head \ "subject").as[String], "route-1")
    assertEquals(ext(events.head, "partitionkey"), Some("route-1"))
    assertEquals(numbers(events), Seq(0, 1))
  }

  test("the data exporter, raw: the event alone") {
    implicit val env: otoroshi.env.Env = Gateway.instance.env
    val topic  = uniq("raw-exported")
    val result = await(new EventBrokersDataExporter().send(Seq(otoroshiEvent(5)), CustomDataExporterContext(Json.obj("connection" -> kafka, "options" -> Json.obj("topic" -> topic), "format" -> "raw"), null)))
    assertEquals(result, ExportResult.ExportResultSuccess)
    val event  = read(topic, 1).head
    // no attributes went with it: read as a message of another producer
    assertEquals((event \ "type").as[String], "com.cloud-apim.event-brokers.message")
    assertEquals((payload(event) \ "@id").as[String], "event-5")
  }

  test("the data exporter says what goes wrong") {
    implicit val env: otoroshi.env.Env = Gateway.instance.env
    val result = await(new EventBrokersDataExporter().send(Seq(otoroshiEvent(0)), CustomDataExporterContext(Json.obj("connection" -> "nope"), null)))
    assert(result.isInstanceOf[ExportResult.ExportResultFailure], result.toString)
  }

  test("workflow functions: publish, then consume") {
    implicit val env: otoroshi.env.Env = Gateway.instance.env
    val topic   = uniq("workflow")
    val publish = WorkflowFunction.get(EventBrokersWorkflows.Publish).get
    val consume = WorkflowFunction.get(EventBrokersWorkflows.Consume).get
    val done    = await(publish.call(Json.obj("connection" -> kafka, "options" -> Json.obj("topic" -> topic), "data" -> message(3, "wf"), "type" -> "com.acme.thing")))
    assertEquals(done.map(r => (r \ "published").as[Boolean]), Right(true))
    val got     = await(consume.call(Json.obj("connection" -> kafka, "options" -> Json.obj("topic" -> topic, "from" -> "earliest"), "take" -> 1, "within_ms" -> 20000)))
    val events  = got.map(r => (r \ "events").as[Seq[JsValue]]).getOrElse(fail(got.toString))
    assertEquals(numbers(events), Seq(3))
    assertEquals((events.head \ "type").as[String], "com.acme.thing")
    assert(await(publish.call(Json.obj("options" -> Json.obj()))).isLeft)
  }

  /** answers each request of `requests` on `replies`, with its data echoed, until `count` are answered */
  def responder(requests: String, count: Int, status: Option[Int] = None): scala.concurrent.Future[Seq[JsValue]] =
    Gateway.ndjson(consumer, "/" + opts("topic" -> requests, "from" -> "earliest")).take(count.toLong).mapAsync(1) { request =>
      val reply = Json.obj(
        "specversion"     -> "1.0",
        "id"              -> uniq("reply"),
        "source"          -> "/responder",
        "type"            -> "reply",
        "correlationid"   -> (request \ "correlationid").as[String],
        "datacontenttype" -> "application/json",
        "data"            -> Json.obj("echo" -> payload(request))
      ) ++ status.map(s => Json.obj("status" -> s.toString)).getOrElse(Json.obj())
      val (code, lines) = Gateway.post(ingester, "/" + opts("topic" -> (request \ "replyto").as[String]), Json.stringify(reply), ctype = "application/cloudevents+json")
      assertEquals(code, 200, lines.toString)
      scala.concurrent.Future.successful(request)
    }.runWith(org.apache.pekko.stream.scaladsl.Sink.seq)

  def requestReply(connection: String, options: JsObject) = Gateway.brokerRoute[HttpEventsRequestReply](connection, Json.obj("options" -> options))

  test("request reply: the request goes out as an event, the response is the reply's data") {
    val requests = uniq("requests")
    val replies  = uniq("replies")
    val route    = requestReply(kafka, Json.obj("topic" -> requests, "reply_to" -> replies, "timeout" -> "20000"))
    val answered = responder(requests, 2)
    (0 until 2).foreach { n =>
      val (status, body) = Gateway.post(route, "/", Json.stringify(message(n, "rr")))
      assertEquals(status, 200, body.toString)
      assertEquals((body.head \ "echo"), JsDefined(message(n, "rr")))
    }
    val seen     = await(answered)
    assert(seen.forall(r => (r \ "replyto").asOpt[String].contains(replies) && (r \ "correlationid").asOpt[String].nonEmpty), seen.toString)
  }

  test("request reply: a reply says its status, no reply is a 504") {
    val requests = uniq("requests")
    val route    = requestReply(kafka, Json.obj("topic" -> requests, "reply_to" -> uniq("replies"), "timeout" -> "20000"))
    responder(requests, 1, status = Some(201))
    assertEquals(Gateway.post(route, "/", Json.stringify(message(0, "rr")))._1, 201)
    val silent   = requestReply(kafka, Json.obj("topic" -> uniq("nobody"), "reply_to" -> uniq("replies"), "timeout" -> "1500"))
    val (status, body) = Gateway.post(silent, "/", Json.stringify(message(0, "rr")))
    assertEquals(status, 504, body.toString)
    assert(body.toString.contains("no_reply"), body.toString)
  }

  test("request reply over rabbitmq: the amqp reply_to and correlation_id are set too") {
    val rabbit   = Gateway.createConnection("rabbitmq", Json.obj(
      "addresses" -> Json.arr(s"${Brokers.rabbitmq.getHost}:${Brokers.rabbitmq.getAmqpPort}"),
      "username"  -> Brokers.rabbitmq.getAdminUsername,
      "password"  -> Brokers.rabbitmq.getAdminPassword,
      "topology"  -> Json.obj("queues" -> Json.arr(Json.obj("name" -> "rr-requests", "durable" -> false)))
    ))
    val replies  = uniq("rr-replies")
    val route    = requestReply(rabbit, Json.obj("queue" -> "rr-requests", "reply_to" -> replies, "reply_options" -> Json.stringify(Json.obj("exchange" -> "amq.direct", "routing_key" -> replies)), "timeout" -> "20000"))
    val in       = Gateway.brokerRoute[HttpEventsIngester](rabbit)
    val out      = Gateway.brokerRoute[HttpEventsConsumer](rabbit)
    val answered = Gateway.ndjson(out, "/" + opts("queue" -> "rr-requests")).take(1).mapAsync(1) { request =>
      val reply = Json.obj("specversion" -> "1.0", "id" -> uniq("reply"), "source" -> "/responder", "type" -> "reply", "correlationid" -> (request \ "correlationid").as[String], "data" -> "pong")
      Gateway.post(in, "/" + opts("exchange" -> "amq.direct", "routing_key" -> (request \ "rabbitmqreplyto").as[String]), Json.stringify(reply), ctype = "application/cloudevents+json")
      scala.concurrent.Future.successful(request)
    }.runWith(org.apache.pekko.stream.scaladsl.Sink.head)
    val (status, body) = Gateway.post(route, "/", Json.stringify(message(0, "amqp")))
    assertEquals(status, 200, body.toString)
    val request  = await(answered)
    assertEquals(ext(request, "rabbitmqreplyto"), Some(replies))
    assertEquals(ext(request, "rabbitmqcorrelationid"), ext(request, "correlationid"))
  }
}
