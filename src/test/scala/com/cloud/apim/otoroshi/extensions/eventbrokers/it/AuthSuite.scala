package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import scala.concurrent.duration.*

/** authentication against every broker: who it knows gets in, nobody else */
class AuthSuite extends BrokerSuite {

  override val munitTimeout: Duration = 10.minutes

  override def startBroker(): Unit = ()

  override def afterAll(): Unit = {
    super.afterAll()
    if (!munitIgnore) AuthBrokers.stopAll()
  }

  /**
   * a connection that cannot connect, and what it says once it says what is expected (a first attempt
   * that times out on a busy machine says less than the next ones)
   */
  def unavailable(kind: String, settings: JsObject, saying: String => Boolean = _.nonEmpty): String = {
    val id    = Gateway.createConnection(kind, settings, waitConnected = false)
    def error = (Gateway.connectionStatus(id).json \ "error").asOpt[String].getOrElse("")
    try {
      eventually("unavailable, saying why", 60.seconds)((Gateway.connectionStatus(id).json \ "state").asOpt[String].contains("unavailable") && saying(error))
      error
    } catch {
      case e: RuntimeException => fail(s"${e.getMessage}: ${Gateway.connectionStatus(id).json}")
    } finally Gateway.deleteConnection(id)
  }

  /** publishes three events and reads them back, the broker keeping them meanwhile */
  def roundTrip(connection: String, options: (String, Any)*)(read: (String, Any)*): Unit = {
    val ingester = Gateway.brokerRoute[HttpEventsIngester](connection)
    val consumer = Gateway.brokerRoute[HttpEventsConsumer](connection)
    val run      = uniq("run")
    (0 until 3).foreach { n =>
      val (status, lines) = Gateway.post(ingester, "/" + opts(options*), Json.stringify(message(n, run)))
      assertEquals(status, 200, lines.toString)
      assertEquals((lines.head \ "published").asOpt[Boolean], Some(true), lines.toString)
    }
    assertEquals(numbers(await(collect(Gateway.ndjson(consumer, "/" + opts((options ++ read)*)), take = 3))), Seq(0, 1, 2))
  }

  def kafka(mechanism: String, username: String, password: String): JsObject = {
    AuthBrokers.kafka
    Json.obj(
      "bootstrap_servers" -> Json.arr(s"localhost:${AuthBrokers.kafkaPort}"),
      "security_protocol" -> "SASL_PLAINTEXT",
      "sasl"              -> Json.obj("mechanism" -> mechanism, "username" -> username, "password" -> password)
    )
  }

  test("kafka: sasl scram-sha-512") {
    roundTrip(Gateway.createConnection("kafka", kafka("SCRAM-SHA-512", "alice", "alice-secret")), "topic" -> uniq("scram"))("from" -> "earliest")
  }

  test("kafka: sasl plain") {
    roundTrip(Gateway.createConnection("kafka", kafka("PLAIN", "bob", "bob-secret")), "topic" -> uniq("plain"))("from" -> "earliest")
  }

  test("kafka: a wrong password is refused") {
    unavailable("kafka", kafka("SCRAM-SHA-512", "alice", "nope"), _.toLowerCase.contains("authentication"))
  }

  def pulsar(auth: JsObject): JsObject = Json.obj("service_url" -> s"pulsar://localhost:${AuthBrokers.pulsar.getMappedPort(6650)}", "auth" -> auth)

  test("pulsar: a token") {
    roundTrip(Gateway.createConnection("pulsar", pulsar(Json.obj("kind" -> "token", "token" -> AuthBrokers.token("otoroshi")))), "topic" -> uniq("token"))("from" -> "earliest")
  }

  test("pulsar: no token, or a token signed with another key, is refused") {
    assert(unavailable("pulsar", pulsar(Json.obj("kind" -> "none"))).nonEmpty)
    val forged = AuthBrokers.token("otoroshi", key = Array.fill[Byte](32)(7))
    assert(unavailable("pulsar", pulsar(Json.obj("kind" -> "token", "token" -> forged))).nonEmpty)
  }

  def redis(more: (String, Json.JsValueWrapper)*): JsObject =
    Json.obj("uri" -> s"redis://localhost:${AuthBrokers.redis.getMappedPort(6379)}", "mode" -> "streams") ++ Json.obj(more*)

  test("redis: an acl user") {
    roundTrip(Gateway.createConnection("redis", redis("username" -> "alice", "password" -> "alice-secret")), "stream" -> uniq("acl"))("from" -> "earliest")
  }

  test("redis: a wrong password, or no user at all, is refused") {
    unavailable("redis", redis("username" -> "alice", "password" -> "nope"), _.contains("WRONGPASS"))
    unavailable("redis", redis(), _.contains("NOAUTH"))
  }

  def mqtt(more: (String, Json.JsValueWrapper)*): JsObject =
    Json.obj("host" -> "localhost", "port" -> AuthBrokers.mosquitto.getMappedPort(1883).intValue()) ++ Json.obj(more*)

  test("mqtt: a user of the password file") {
    val connection = Gateway.createConnection("mqtt", mqtt("username" -> "alice", "password" -> "alice-secret"))
    val ingester   = Gateway.brokerRoute[HttpEventsIngester](connection)
    val consumer   = Gateway.brokerRoute[HttpEventsConsumer](connection)
    val topic      = uniq("auth")
    val run        = uniq("run")
    val received   = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3)
    keepPublishing(received)(n => Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(n, run))))
    assertConsecutive(numbers(await(received)), 3)
  }

  test("mqtt: a wrong password is refused") {
    assert(unavailable("mqtt", mqtt("username" -> "alice", "password" -> "nope")).nonEmpty)
  }

  test("rabbitmq: a wrong password is refused") {
    val broker = Brokers.rabbitmq
    unavailable(
      "rabbitmq",
      Json.obj("addresses" -> Json.arr(s"localhost:${broker.getAmqpPort}"), "username" -> broker.getAdminUsername, "password" -> "nope"),
      e => e.contains("ACCESS_REFUSED") || e.toLowerCase.contains("refused")
    )
  }
}
