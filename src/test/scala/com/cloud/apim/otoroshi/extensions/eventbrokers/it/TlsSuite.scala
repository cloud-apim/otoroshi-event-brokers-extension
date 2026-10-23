package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import otoroshi.ssl.DynamicSSLEngineProvider
import play.api.libs.ws.JsonBodyWritables.writeableOf_JsValue
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.Base64
import scala.concurrent.duration.*

/**
 * Tls against every broker, with the certificates of the otoroshi store: each broker asks for a client
 * certificate signed by the test ca, and presents a certificate of that ca for `localhost`.
 */
class TlsSuite extends BrokerSuite {

  import Gateway.given

  override val munitTimeout: Duration = 10.minutes

  override def startBroker(): Unit = { certificates; () }

  val CaId     = "event-brokers-tests-ca"
  val ClientId = "event-brokers-tests-client"

  /** the ca and the client certificate, created through the admin api like an operator would */
  lazy val certificates: Unit = {
    Seq(
      Json.obj("id" -> CaId, "name" -> "event brokers tests ca", "domain" -> "event brokers tests ca", "chain" -> Pki.ca.certPem, "privateKey" -> "", "ca" -> true),
      Json.obj("id" -> ClientId, "name" -> "event brokers tests client", "domain" -> "otoroshi", "chain" -> Pki.client.chainPem, "privateKey" -> Pki.client.keyPem, "client" -> true)
    ).foreach { cert =>
      val res = await(Gateway.admin("/api/certificates").post(cert: JsValue))
      assert(res.status < 300, res.body)
    }
    eventually("the certificates in the store")(Seq(CaId, ClientId).forall(DynamicSSLEngineProvider.certificates.contains))
  }

  def tls(more: (String, Json.JsValueWrapper)*): JsObject =
    Json.obj("enabled" -> true, "client_certs" -> Json.arr(ClientId), "trusted_certs" -> Json.arr(CaId)) ++ Json.obj(more*)

  /** a connection that cannot connect, and what it says */
  def unavailable(kind: String, settings: JsObject): String = {
    val id = Gateway.createConnection(kind, settings, waitConnected = false)
    eventually("unavailable")((Gateway.connectionStatus(id).json \ "state").asOpt[String].contains("unavailable"))
    val error = (Gateway.connectionStatus(id).json \ "error").asOpt[String].getOrElse("")
    Gateway.deleteConnection(id)
    error
  }

  /** publishes three events through the gateway, and reads them back: the broker keeps them meanwhile */
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

  def redis(uri: String, tlsSettings: JsObject): JsObject = Json.obj("uri" -> uri, "mode" -> "streams", "tls" -> tlsSettings)

  def redisUri(host: String = "localhost"): String = s"rediss://$host:${TlsBrokers.redis.getMappedPort(6379)}"

  test("redis over mutual tls, with the certificates of the store") {
    roundTrip(Gateway.createConnection("redis", redis(redisUri(), tls())), "stream" -> uniq("tls"))("from" -> "earliest")
  }

  test("redis: without a client certificate, the broker says no") {
    val error = unavailable("redis", redis(redisUri(), tls("client_certs" -> Json.arr())))
    assert(error.nonEmpty, error)
  }

  test("redis: without the ca, the broker is not trusted") {
    val error = unavailable("redis", redis(redisUri(), tls("trusted_certs" -> Json.arr())))
    assert(error.toLowerCase.contains("certification path") || error.toLowerCase.contains("pkix"), error)
  }

  test("redis: trust_all trusts a broker whatever its certificate") {
    roundTrip(Gateway.createConnection("redis", redis(redisUri(), tls("trusted_certs" -> Json.arr(), "trust_all" -> true))), "stream" -> uniq("trust-all"))("from" -> "earliest")
  }

  test("redis: a name the certificate does not hold fails, unless hostname_verification is off") {
    val error = unavailable("redis", redis(redisUri("127.0.0.1"), tls()))
    assert(error.toLowerCase.contains("subject alternative") || error.toLowerCase.contains("hostname") || error.toLowerCase.contains("127.0.0.1"), error)
    roundTrip(Gateway.createConnection("redis", redis(redisUri("127.0.0.1"), tls("hostname_verification" -> false))), "stream" -> uniq("ip"))("from" -> "earliest")
  }

  test("a certificate the store does not have: the connection says which") {
    val error = unavailable("redis", redis(redisUri(), tls("client_certs" -> Json.arr("nope"))))
    assert(error.contains("'nope' is not in the certificate store"), error)
  }

  test("mqtt over mutual tls") {
    val connection = Gateway.createConnection("mqtt", Json.obj("host" -> "localhost", "port" -> TlsBrokers.mosquitto.getMappedPort(8883).intValue(), "tls" -> tls()))
    val ingester   = Gateway.brokerRoute[HttpEventsIngester](connection)
    val consumer   = Gateway.brokerRoute[HttpEventsConsumer](connection)
    val topic      = uniq("tls")
    val run        = uniq("run")
    val received   = collect(Gateway.ndjson(consumer, "/" + opts("topic" -> topic)), take = 3)
    keepPublishing(received)(n => Gateway.post(ingester, "/" + opts("topic" -> topic), Json.stringify(message(n, run))))
    assertConsecutive(numbers(await(received)), 3)
    assert(unavailable("mqtt", Json.obj("host" -> "localhost", "port" -> TlsBrokers.mosquitto.getMappedPort(8883).intValue(), "tls" -> tls("client_certs" -> Json.arr()))).nonEmpty)
  }

  test("rabbitmq over mutual tls") {
    val broker = TlsBrokers.rabbitmq
    val queue  = uniq("tls")
    // the management api, without tls, makes the queue
    val auth   = Base64.getEncoder.encodeToString(s"${broker.getAdminUsername}:${broker.getAdminPassword}".getBytes)
    val res    = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build().send(
      HttpRequest.newBuilder(URI.create(s"${broker.getHttpUrl}/api/queues/%2F/$queue")).header("Authorization", s"Basic $auth").header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString("""{"durable":false}""")).build(),
      HttpResponse.BodyHandlers.ofString()
    )
    assert(res.statusCode() < 300, res.body())
    val settings = Json.obj("addresses" -> Json.arr(s"localhost:${broker.getAmqpsPort}"), "username" -> broker.getAdminUsername, "password" -> broker.getAdminPassword, "tls" -> tls())
    roundTrip(Gateway.createConnection("rabbitmq", settings), "queue" -> queue)()
    assert(unavailable("rabbitmq", settings ++ Json.obj("tls" -> tls("client_certs" -> Json.arr()))).nonEmpty)
  }

  test("artemis over mutual tls") {
    val settings = Json.obj("url" -> s"tcp://localhost:${TlsBrokers.artemis.getMappedPort(61617)}", "username" -> "artemis", "password" -> "artemis", "tls" -> tls())
    roundTrip(Gateway.createConnection("artemis", settings), "address" -> uniq("tls"), "routing_type" -> "anycast")()
    assert(unavailable("artemis", settings ++ Json.obj("tls" -> tls("client_certs" -> Json.arr()))).nonEmpty)
  }

  test("kafka over mutual tls, the certificates given as pem") {
    TlsBrokers.kafka
    val settings = Json.obj("bootstrap_servers" -> Json.arr(s"localhost:${TlsBrokers.kafkaPort}"), "security_protocol" -> "SSL", "tls" -> tls())
    roundTrip(Gateway.createConnection("kafka", settings), "topic" -> uniq("tls"))("from" -> "earliest")
    assert(unavailable("kafka", settings ++ Json.obj("tls" -> tls("client_certs" -> Json.arr()))).nonEmpty)
  }

  test("pulsar over mutual tls") {
    val settings = Json.obj("service_url" -> s"pulsar+ssl://localhost:${TlsBrokers.pulsar.getMappedPort(6651)}", "tls" -> tls())
    roundTrip(Gateway.createConnection("pulsar", settings), "topic" -> uniq("tls"))("from" -> "earliest")
    assert(unavailable("pulsar", settings ++ Json.obj("tls" -> tls("client_certs" -> Json.arr()))).nonEmpty)
  }
}
