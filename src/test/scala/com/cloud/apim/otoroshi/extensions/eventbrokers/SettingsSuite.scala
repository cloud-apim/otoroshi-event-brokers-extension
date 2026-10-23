package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.*
import otoroshi.models.EntityLocation
import play.api.libs.json.*

class SettingsSuite extends munit.FunSuite {

  def read(kind: EventBrokerConnectionKind, json: JsObject): Either[Seq[String], BrokerSettings] = BrokerSettings.read(kind, json)

  def errors(kind: EventBrokerConnectionKind, json: JsObject): Seq[String] = read(kind, json).left.getOrElse(Seq.empty)

  def assertError(kind: EventBrokerConnectionKind, json: JsObject, expected: String)(using munit.Location): Unit = {
    val errs = errors(kind, json)
    assert(errs.exists(_.startsWith(expected)), s"expected an error starting with '$expected', got $errs")
  }

  import EventBrokerConnectionKind.*

  test("the template of every kind reads back to itself") {
    EventBrokerConnectionKind.values.foreach { kind =>
      val template = BrokerSettings.template(kind)
      assertEquals(read(kind, template.json), Right(template), kind.name)
    }
  }

  test("what is not required has a default") {
    assertEquals(read(Kafka, Json.obj("bootstrap_servers" -> Json.arr("k:9092"))), Right(KafkaSettings(bootstrapServers = Seq("k:9092"))))
    assertEquals(read(RabbitMQ, Json.obj("addresses" -> Json.arr("r"))), Right(RabbitMQSettings(addresses = Seq("r"))))
    assertEquals(read(Pulsar, Json.obj("service_url" -> "pulsar://p:6650")), Right(PulsarSettings(serviceUrl = "pulsar://p:6650")))
    assertEquals(read(Redis, Json.obj("uri" -> "redis://r:6379")), Right(RedisSettings(uri = "redis://r:6379")))
    assertEquals(read(MQTT, Json.obj("host" -> "m")), Right(MqttSettings(host = "m")))
    // published messages are not retained unless asked: a retained message is replayed to every new subscriber
    assertEquals(MqttSettings(host = "m").retain, false)
  }

  test("what is required is required") {
    assertError(Kafka, Json.obj(), "bootstrap_servers: required")
    assertError(RabbitMQ, Json.obj(), "addresses: required")
    assertError(Pulsar, Json.obj(), "service_url: required")
    assertError(Redis, Json.obj(), "uri: required")
    assertError(MQTT, Json.obj(), "host: required")
    assertError(Kafka, Json.obj("bootstrap_servers" -> Json.arr()), "bootstrap_servers: must not be empty")
    assertError(MQTT, Json.obj("host" -> " "), "host: must not be empty")
  }

  test("the settings of the previous format are refused, field by field") {
    val errs = errors(Kafka, Json.obj("servers" -> Json.arr("k:9092"), "topic" -> "t", "securityProtocol" -> "PLAINTEXT"))
    assertEquals(errs.toSet, Set("bootstrap_servers: required", "securityProtocol: unknown field", "servers: unknown field", "topic: unknown field"))
    assertError(Redis, Json.obj("uri" -> "redis://r", "uris" -> Json.arr("redis://r")), "uris: unknown field")
    assertError(MQTT, Json.obj("host" -> "m", "publisher_id" -> "x"), "publisher_id: unknown field")
  }

  test("errors say where, nested fields included") {
    assertError(MQTT, Json.obj("host" -> "m", "port" -> "abc"), "port: expected")
    assertError(Kafka, Json.obj("bootstrap_servers" -> Json.arr("k:9092"), "sasl" -> Json.obj("mechanism" -> "MAGIC")), "sasl.mechanism: must be one of")
    assertError(Kafka, Json.obj("bootstrap_servers" -> Json.arr("k:9092"), "tls" -> Json.obj("nope" -> true)), "tls.nope: unknown field")
    assertError(Pulsar, Json.obj("service_url" -> "pulsar://p", "auth" -> "token"), "auth: expected an object")
  }

  test("kafka: servers, sasl and tls are checked") {
    val servers = Json.obj("bootstrap_servers" -> Json.arr("k:9092"))
    assertError(Kafka, Json.obj("bootstrap_servers" -> Json.arr("localhost")), "bootstrap_servers: 'localhost' is not a host:port")
    assertError(Kafka, Json.obj("bootstrap_servers" -> Json.arr("PLAINTEXT://k:9092")), "bootstrap_servers: 'PLAINTEXT://k:9092'")
    assertError(Kafka, servers ++ Json.obj("security_protocol" -> "SASL_SSL"), "sasl: SASL_SSL needs a username and a password")
    assert(read(Kafka, servers ++ Json.obj("security_protocol" -> "SASL_SSL", "sasl" -> Json.obj("jaas_config" -> "x required;"))).isRight)
    assertError(Kafka, servers ++ Json.obj("tls" -> Json.obj("enabled" -> true)), "tls: tls needs security_protocol SSL or SASL_SSL")
    assertError(Kafka, servers ++ Json.obj("security_protocol" -> "SSL", "tls" -> Json.obj("keystore_path" -> "/k.jks")), "tls: keystore_path, truststore_path and store_password go together")
    assertError(Kafka, servers ++ Json.obj("security_protocol" -> "TLS"), "security_protocol: must be one of")
  }

  test("kafka: what otoroshi's kafka support reads") {
    val settings = KafkaSettings(
      bootstrapServers = Seq("a:1", "b:2"),
      defaultTopic = Some("t"),
      securityProtocol = "SASL_SSL",
      sasl = KafkaSasl(mechanism = "SCRAM-SHA-512", username = Some("u"), password = Some("p")),
      tls = KafkaTls(tls = TlsSettings(enabled = true, trustedCerts = Seq("ca"), hostnameVerification = false)),
      properties = Map("acks" -> "all")
    )
    val config   = settings.kafkaConfig
    assertEquals(config.servers, Seq("a:1", "b:2"))
    assertEquals(config.topic, "t")
    assertEquals(config.saslConfig.map(s => (s.username, s.password, s.mechanism)), Some(("u", "p", "SCRAM-SHA-512")))
    assertEquals(config.mtlsConfig.mtls, true)
    assertEquals(config.mtlsConfig.trustedCerts, Seq("ca"))
    assertEquals(config.mtlsConfig.loose, true)
    assertEquals(config.hostValidation, false)
    assertEquals(config.properties, Some(Map("acks" -> "all")))
    // no sasl configuration without a sasl protocol
    assertEquals(KafkaSettings(bootstrapServers = Seq("a:1"), sasl = KafkaSasl(username = Some("u"))).kafkaConfig.saslConfig, None)
  }

  test("pulsar: the url, the authentication and the topics") {
    assertError(Pulsar, Json.obj("service_url" -> "http://p:8080"), "service_url: must start with pulsar:// or pulsar+ssl://")
    assertError(Pulsar, Json.obj("service_url" -> "pulsar://p", "tls" -> Json.obj("enabled" -> true)), "tls: tls needs a pulsar+ssl:// service_url")
    assertError(Pulsar, Json.obj("service_url" -> "pulsar://p", "auth" -> Json.obj("kind" -> "token")), "auth.token: required")
    assertError(Pulsar, Json.obj("service_url" -> "pulsar://p", "auth" -> Json.obj("kind" -> "basic", "username" -> "u")), "auth.password: required")
    // the form keeps the fields of the other kinds of authentication
    assertEquals(
      read(Pulsar, Json.obj("service_url" -> "pulsar://p", "auth" -> Json.obj("kind" -> "token", "token" -> "t", "username" -> "", "password" -> ""))),
      Right(PulsarSettings(serviceUrl = "pulsar://p", auth = PulsarAuth.Token("t")))
    )
    val settings = PulsarSettings(serviceUrl = "pulsar://p", tenant = "acme", namespace = "orders")
    assertEquals(settings.fullTopic("created"), "persistent://acme/orders/created")
    assertEquals(settings.fullTopic("non-persistent://other/ns/t"), "non-persistent://other/ns/t")
  }

  test("redis and mqtt: what is checked") {
    assertError(Redis, Json.obj("uri" -> "http://r"), "uri: is not a redis uri")
    assert(read(Redis, Json.obj("uri" -> "rediss://r:6380", "tls" -> Json.obj("hostname_verification" -> false))).isRight)
    assertError(Redis, Json.obj("uri" -> "rediss://r:6380", "tls" -> Json.obj("verify_peer" -> false)), "tls.verify_peer: unknown field")
    assert(RedisSettings(uri = "rediss://r:6380").usesTls)
    assert(RedisSettings(uri = "redis://r:6379", tls = TlsSettings(enabled = true)).usesTls)
    assert(!RedisSettings(uri = "redis://r:6379").usesTls)
    assertError(MQTT, Json.obj("host" -> "m", "port" -> 0), "port: must be between 1 and 65535")
    assertError(MQTT, Json.obj("host" -> "m", "default_qos" -> 3), "default_qos: must be between 0 and 2")
    assertError(MQTT, Json.obj("host" -> "m", "client_id_prefix" -> "a-much-too-long-prefix"), "client_id_prefix: at most 14")
    assertError(MQTT, Json.obj("host" -> "m", "version" -> 4), "version: must be 3 (mqtt 3.1.1) or 5")
    assertError(MQTT, Json.obj("host" -> "m", "version" -> 3, "session_expiry_seconds" -> 60), "session_expiry_seconds: is an mqtt 5 setting")
    assertError(MQTT, Json.obj("host" -> "m", "transport" -> "quic"), "transport: must be one of tcp, websocket")
    assertError(MQTT, Json.obj("host" -> "m", "websocket_path" -> "mqtt"), "websocket_path: must start with /")
    assertEquals(MqttSettings(host = "m").version, 5)
  }

  test("redis streams") {
    val streams = Json.obj("default_stream" -> "orders", "max_length" -> 1000, "exact_trimming" -> true, "data_field" -> "payload", "claim_min_idle_ms" -> 5000)
    assertEquals(
      read(Redis, Json.obj("uri" -> "redis://r", "mode" -> "streams", "streams" -> streams)),
      Right(RedisSettings(uri = "redis://r", mode = "streams", streams = RedisStreamsSettings(Some("orders"), Some(1000L), true, "payload", 5000L)))
    )
    assertEquals(RedisSettings(uri = "redis://r").isStreams, false)
    assertError(Redis, Json.obj("uri" -> "redis://r", "mode" -> "list"), "mode: must be one of pubsub, streams")
    assertError(Redis, Json.obj("uri" -> "redis://r", "streams" -> Json.obj("max_length" -> -1)), "streams.max_length: must not be negative")
    assertEquals(read(Redis, Json.obj("uri" -> "redis://r", "streams" -> Json.obj("max_length" -> 0))).map(_.asInstanceOf[RedisSettings].streams.maxLength), Right(None))
    assertError(Redis, Json.obj("uri" -> "redis://r", "streams" -> Json.obj("claim_min_idle_ms" -> -1)), "streams.claim_min_idle_ms: must not be negative")
    assertError(Redis, Json.obj("uri" -> "redis://r", "streams" -> Json.obj("data_field" -> "")), "streams.data_field: must not be empty")
    assertError(Redis, Json.obj("uri" -> "redis://r", "streams" -> Json.obj("maxlen" -> 10)), "streams.maxlen: unknown field")
  }

  test("tls, the same everywhere") {
    val tls = Json.obj("enabled" -> true, "client_certs" -> Json.arr("client"), "trusted_certs" -> Json.arr("ca"), "hostname_verification" -> false)
    val expected = TlsSettings(enabled = true, clientCerts = Seq("client"), trustedCerts = Seq("ca"), hostnameVerification = false)
    assertEquals(read(RabbitMQ, Json.obj("addresses" -> Json.arr("r"), "tls" -> tls)).map(_.asInstanceOf[RabbitMQSettings].tls), Right(expected))
    assertEquals(read(Artemis, Json.obj("url" -> "tcp://a:61617", "tls" -> tls)).map(_.asInstanceOf[ArtemisSettings].tls), Right(expected))
    assertEquals(read(Redis, Json.obj("uri" -> "redis://r", "tls" -> tls)).map(_.asInstanceOf[RedisSettings].tls), Right(expected))
    assertEquals(read(MQTT, Json.obj("host" -> "m", "tls" -> tls)).map(_.asInstanceOf[MqttSettings].tls), Right(expected))
    // kafka has no trust-all, and its certificates come from the store or from files
    val kafka = Json.obj("bootstrap_servers" -> Json.arr("k:9093"), "security_protocol" -> "SSL")
    assertError(Kafka, kafka ++ Json.obj("tls" -> Json.obj("enabled" -> true, "trust_all" -> true)), "tls.trust_all: kafka cannot trust every certificate")
    assertError(
      Kafka,
      kafka ++ Json.obj("tls" -> Json.obj("enabled" -> true, "trusted_certs" -> Json.arr("ca"), "keystore_path" -> "/k", "truststore_path" -> "/t", "store_password" -> "p")),
      "tls: the certificates come from the store"
    )
  }

  test("artemis") {
    assertEquals(read(Artemis, Json.obj("url" -> "tcp://a:61616")), Right(ArtemisSettings(url = "tcp://a:61616")))
    assertEquals(ArtemisSettings(url = "tcp://a:61616").defaultRoutingType, "multicast")
    assert(read(Artemis, Json.obj("url" -> "(tcp://a:61616,tcp://b:61616)?ha=true&sslEnabled=true")).isRight)
    assertError(Artemis, Json.obj(), "url: required")
    assertError(Artemis, Json.obj("url" -> "http://a"), "url: is not an artemis core url")
    assertError(Artemis, Json.obj("url" -> "tcp://a", "default_routing_type" -> "broadcast"), "default_routing_type: must be one of multicast, anycast")
    assertError(Artemis, Json.obj("url" -> "tcp://a", "consumer_window_size" -> -2), "consumer_window_size: must be -1")
    assertError(Artemis, Json.obj("url" -> "tcp://a", "reconnect_attempts" -> -5), "reconnect_attempts: must be -1")
    assertError(Artemis, Json.obj("url" -> "tcp://a", "call_timeout_ms" -> 1), "call_timeout_ms: must be at least 100")
    assert(read(Artemis, Json.obj("url" -> "${vault://local/artemis/url}")).isRight)
  }

  test("values that come from a vault are only checked once resolved") {
    assert(read(Kafka, Json.obj("bootstrap_servers" -> Json.arr("${vault://local/kafka/servers}"))).isRight)
    assert(read(Redis, Json.obj("uri" -> "${vault://local/redis/uri}")).isRight)
    assert(read(Pulsar, Json.obj("service_url" -> "${vault://local/pulsar/url}")).isRight)
    assert(read(RabbitMQ, Json.obj("addresses" -> Json.arr("${vault://local/rabbit/address}"), "password" -> "${vault://local/rabbit/password}")).isRight)
  }

  def connection(kind: EventBrokerConnectionKind, settings: JsObject): EventBrokerConnection = EventBrokerConnection(
    location = EntityLocation.default,
    id = "c1",
    name = "c1",
    description = "",
    tags = Seq.empty,
    metadata = Map.empty,
    kind = kind,
    settings = settings
  )

  test("the admin api refuses an unknown kind, and settings that do not fit the kind") {
    val valid = connection(Redis, Json.obj("uri" -> "redis://r"))
    assertEquals(EventBrokerConnection.validate(valid, valid.json), Right(valid))
    assertEquals(EventBrokerConnection.validate(valid, valid.json.as[JsObject] - "kind"), Left(Seq("kind: required")))
    assert(EventBrokerConnection.validate(valid, valid.json.as[JsObject] ++ Json.obj("kind" -> "nats")).left.exists(_.head.startsWith("kind: must be one of")))
    val invalid = connection(Redis, Json.obj("uris" -> Json.arr("redis://r")))
    assertEquals(EventBrokerConnection.validate(invalid, invalid.json).left.map(_.toSet), Left(Set("settings.uri: required", "settings.uris: unknown field")))
  }

  test("a connection with invalid settings builds no client, and says why") {
    val invalid = connection(Kafka, Json.obj())
    val e       = intercept[InvalidConnectionSettings](invalid.newClientInstance()(using null))
    assertEquals(e.errors, Seq("bootstrap_servers: required"))
  }
}
