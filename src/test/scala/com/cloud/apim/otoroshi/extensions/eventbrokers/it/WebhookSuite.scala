package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.*
import io.lettuce.core.RedisClient
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.util.ByteString
import otoroshi.models.EntityLocation
import otoroshi.storage.drivers.lettuce.ByteStringRedisCodec
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.*

/** events of a connection pushed to http endpoints */
class WebhookSuite extends BrokerSuite {

  import Gateway.given

  override def startBroker(): Unit = { Brokers.kafka; Brokers.redis; () }

  lazy val connection: String = Gateway.createConnection("kafka", Json.obj("bootstrap_servers" -> Json.arr(Brokers.kafka.getBootstrapServers)))
  lazy val ingester           = Gateway.brokerRoute[HttpEventsIngester](connection)
  lazy val consumer           = Gateway.brokerRoute[HttpEventsConsumer](connection)

  def publish(topic: String, n: Int, run: String): Unit = {
    val (status, lines) = Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(n, run)))
    assertEquals(status, 200, lines.toString)
  }

  def webhook(topic: String, endpoint: JsObject, more: (String, Json.JsValueWrapper)*): String =
    Gateway.createWebhook(Json.obj("connection" -> connection, "subscription" -> Json.obj("topic" -> topic, "from" -> "earliest"), "endpoint" -> endpoint) ++ Json.obj(more*))

  def deliveries(target: WebhookTarget, n: Int): Seq[Delivery] = (0 until n).map(i => target.next().getOrElse(fail(s"delivery ${i + 1} of $n did not come")))

  def hmac(secret: String, body: ByteString): String = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))
    "sha256=" + mac.doFinal(body.toArray).map(b => f"$b%02x").mkString
  }

  test("structured: each event, in order, signed, as a cloudevent") {
    val target = new WebhookTarget()
    val topic  = uniq("structured")
    val run    = uniq("run")
    (0 until 3).foreach(n => publish(topic, n, run))
    val id     = webhook(topic, Json.obj("url" -> target.url, "secret" -> "s3cr3t", "headers" -> Json.obj("X-Tenant" -> "acme")))
    try {
      val got = deliveries(target, 3)
      assertEquals(numbers(got.map(d => Json.parse(d.body.toArray))), Seq(0, 1, 2))
      assert(got.forall(_.contentType.startsWith("application/cloudevents+json")), got.map(_.contentType).toString)
      assert(got.forall(d => d.header("X-Event-Brokers-Webhook").contains(id) && d.header("X-Tenant").contains("acme")), got.toString)
      assert(got.forall(d => d.header("X-Event-Brokers-Signature").contains(hmac("s3cr3t", d.body))), got.toString)
      eventually("counted")((Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/webhooks/$id/_status").get().map(_.json \ "delivered").awaited.asOpt[Long]).contains(3L))
    } finally {
      Gateway.deleteWebhook(id)
      target.stop()
    }
  }

  test("binary: the attributes in ce- headers, the data as the body; raw: the data alone") {
    val binary = new WebhookTarget()
    val raw    = new WebhookTarget()
    val topic  = uniq("modes")
    val run    = uniq("run")
    publish(topic, 7, run)
    val ids    = Seq(webhook(topic, Json.obj("url" -> binary.url, "mode" -> "binary")), webhook(topic, Json.obj("url" -> raw.url, "mode" -> "raw")))
    try {
      val b = binary.next().get
      assertEquals(Json.parse(b.body.toArray), message(7, run))
      assertEquals(b.header("ce-specversion"), Some("1.0"))
      assert(b.header("ce-id").nonEmpty && b.header("ce-kafkaoffset").nonEmpty, b.headers.toString)
      assert(b.contentType.startsWith("application/json"), b.contentType)
      val r = raw.next().get
      assertEquals(Json.parse(r.body.toArray), message(7, run))
      assertEquals(r.header("ce-id"), None)
    } finally {
      ids.foreach(Gateway.deleteWebhook)
      binary.stop()
      raw.stop()
    }
  }

  test("an endpoint that fails is tried again, and gets the event once it answers") {
    // two 503 first
    val target = new WebhookTarget(n => if (n <= 2) StatusCodes.ServiceUnavailable else StatusCodes.OK)
    val topic  = uniq("retried")
    publish(topic, 0, uniq("run"))
    val id     = webhook(topic, Json.obj("url" -> target.url), "retry" -> Json.obj("max_attempts" -> 5, "initial_backoff_ms" -> 100, "max_backoff_ms" -> 500))
    try {
      val got = deliveries(target, 3)
      assertEquals(got.map(_.header("X-Event-Brokers-Attempt")), Seq(Some("1"), Some("2"), Some("3")))
      assertEquals(got.map(_.body).distinct.size, 1)
      eventually("counted") {
        val status = Gateway.admin(s"/api/extensions/cloud-apim/event-brokers/webhooks/$id/_status").get().awaited.json
        (status \ "delivered").asOpt[Long].contains(1L) && (status \ "retried").asOpt[Long].contains(2L)
      }
      assertEquals(target.next(2.seconds), None)
    } finally {
      Gateway.deleteWebhook(id)
      target.stop()
    }
  }

  test("an event the endpoint refuses goes to the dead letters, saying why") {
    val target = new WebhookTarget(_ => StatusCodes.BadRequest)
    val topic  = uniq("refused")
    val dlq    = uniq("dlq")
    val run    = uniq("run")
    publish(topic, 3, run)
    val id     = webhook(topic, Json.obj("url" -> target.url), "dead_letter" -> Json.obj("options" -> Json.obj("topic" -> dlq)))
    try {
      val dead = await(collect(Gateway.ndjson(consumer, "/" + opts("topic" -> dlq, "from" -> "earliest")), take = 1, within = 30.seconds))
      assertEquals(numbers(dead), Seq(3))
      assertEquals(ext(dead.head, "webhookid"), Some(id))
      assertEquals(ext(dead.head, "webhookattempts"), Some("1"))
      assert(ext(dead.head, "webhookerror").exists(_.contains("400")), dead.head.toString)
      // refused for good: not tried again
      assertEquals(target.requests, 1)
    } finally {
      Gateway.deleteWebhook(id)
      target.stop()
    }
  }

  test("without dead letters, an event given up on raises an alert") {
    val target = new WebhookTarget(_ => StatusCodes.ServiceUnavailable)
    val topic  = uniq("alert")
    publish(topic, 0, uniq("run"))
    val id     = webhook(topic, Json.obj("url" -> target.url), "retry" -> Json.obj("max_attempts" -> 2, "initial_backoff_ms" -> 50, "max_backoff_ms" -> 100))
    try {
      eventually("alerted") {
        Gateway.events.toArray.toSeq.map(_.asInstanceOf[JsValue]).exists(e => (e \ "alert").asOpt[String].contains("EventBrokerWebhookDeliveryFailed") && (e \ "webhook" \ "id").asOpt[String].contains(id))
      }
      assertEquals(target.requests, 2)
    } finally {
      Gateway.deleteWebhook(id)
      target.stop()
    }
  }

  test("a webhook deleted delivers no more") {
    val target = new WebhookTarget()
    val topic  = uniq("deleted")
    val run    = uniq("run")
    publish(topic, 0, run)
    val id     = webhook(topic, Json.obj("url" -> target.url))
    assert(target.next().isDefined)
    Gateway.deleteWebhook(id)
    eventually("stopped")(Gateway.extension.webhooks.runner(id).isEmpty)
    publish(topic, 1, run)
    assertEquals(target.next(3.seconds), None)
    target.stop()
  }

  test("the admin api refuses a webhook that is not one, saying where") {
    val res = Gateway.admin(Gateway.webhooksPath).post(Json.stringify(Json.obj("id" -> uniq("bad"), "name" -> "bad", "description" -> "", "endpoint" -> Json.obj("url" -> "ftp://x", "mode" -> "soap")))).awaited
    assertEquals(res.status, 400, res.body)
    val errors = (res.json \ "errors").as[Seq[String]]
    assert(errors.contains("connection: required"), errors.toString)
    assert(errors.exists(_.startsWith("endpoint.url: must be an http")), errors.toString)
    assert(errors.exists(_.startsWith("endpoint.mode: must be one of")), errors.toString)
  }

  // -----------------------------------------------------------------------------------------------
  // a cluster: the nodes share their locks in a redis
  // -----------------------------------------------------------------------------------------------

  test("the locks of a cluster: one node holds a webhook, the others wait until it lets go or stops saying it holds it") {
    val prefix = uniq("locks")
    val client = RedisClient.create(Brokers.redisUri)
    val conn   = client.connect(new ByteStringRedisCodec())
    val a      = new RedisWebhookLocks("a", () => conn, 1.second, prefix)
    val b      = new RedisWebhookLocks("b", () => conn, 1.second, prefix)
    try {
      assert(await(a.acquire("w")))
      assert(!await(b.acquire("w")))
      assert(await(a.acquire("w")), "renewed")
      assertEquals(await(b.owner("w")), Some("a"))
      await(a.release("w"))
      assert(await(b.acquire("w")))
      // b stops renewing: once its lock expires, a gets it
      Thread.sleep(1300L)
      assert(await(a.acquire("w")))
      // releasing what another node holds does nothing
      await(b.release("w"))
      assertEquals(await(a.owner("w")), Some("a"))
    } finally {
      conn.close()
      client.shutdown()
    }
  }

  test("two nodes sharing their locks: each event delivered once, and the other node takes over when the first one stops") {
    val target   = new WebhookTarget()
    val topic    = uniq("cluster")
    val run      = uniq("run")
    val prefix   = uniq("cluster-locks")
    val client   = RedisClient.create(Brokers.redisUri)
    val conn     = client.connect(new ByteStringRedisCodec())
    val settings = WebhookManagerSettings(lockTtl = 1500.millis)
    val env      = Gateway.instance.env
    val nodes    = Seq("one", "two").map(name => new WebhookManager(env, settings, Gateway.extension.connections, _ => new RedisWebhookLocks(name, () => conn, settings.lockTtl, prefix)))
    val hook     = EventBrokerWebhook(
      location = EntityLocation.default,
      id = uniq("shared-hook"),
      name = "shared",
      description = "",
      tags = Seq.empty,
      metadata = Map.empty,
      connection = connection,
      subscription = Json.obj("topic" -> topic, "from" -> "earliest"),
      endpoint = WebhookEndpoint(url = target.url)
    )
    try {
      (0 until 5).foreach(n => publish(topic, n, run))
      nodes.foreach(_.sync(Seq(hook)))
      assertEquals(numbers(deliveries(target, 5).map(d => Json.parse(d.body.toArray))), Seq(0, 1, 2, 3, 4))
      assertEquals(target.next(2.seconds), None)
      val delivering = nodes.filter(_.runner(hook.id).exists(_.isDelivering))
      assertEquals(delivering.size, 1)
      // the delivering node goes: the other one takes over where its group stopped
      await(delivering.head.stop())
      (5 until 8).foreach(n => publish(topic, n, run))
      assertEquals(numbers(deliveries(target, 3).map(d => Json.parse(d.body.toArray))), Seq(5, 6, 7))
      assert(nodes.filterNot(_ eq delivering.head).forall(_.runner(hook.id).exists(_.isDelivering)))
    } finally {
      nodes.foreach(n => await(n.stop()))
      conn.close()
      client.shutdown()
      target.stop()
    }
  }

  extension [A](f: scala.concurrent.Future[A]) def awaited: A = Gateway.await(f)
}
