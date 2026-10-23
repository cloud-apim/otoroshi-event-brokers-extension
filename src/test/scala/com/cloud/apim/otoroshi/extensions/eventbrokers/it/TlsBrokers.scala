package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.pulsar.PulsarContainer
import org.testcontainers.rabbitmq.RabbitMQContainer
import org.testcontainers.utility.DockerImageName

import java.nio.charset.StandardCharsets

/**
 * The brokers of the tls tests, each one with the server certificate of `Pki`, and each one asking
 * its clients for a certificate signed by the ca (mutual tls). Started on first use, shared.
 */
object TlsBrokers {

  private val started = new java.util.concurrent.CopyOnWriteArrayList[org.testcontainers.containers.GenericContainer[?]]()

  private def started[C <: org.testcontainers.containers.GenericContainer[?]](container: C): C = {
    started.add(container)
    container
  }

  /** stops the brokers started so far: they are only needed by their suite */
  def stopAll(): Unit = {
    started.forEach(c => scala.util.Try(c.stop()))
    started.clear()
  }

  private def text(content: String): Transferable = Transferable.of(content.getBytes(StandardCharsets.UTF_8), 0x1a4) // 0644
  private def binary(content: Array[Byte]): Transferable = Transferable.of(content, 0x1a4)

  def freePort(): Int = DockerSupport.freePort()

  lazy val redis: Container = {
    val container = new Container("redis:7.4-alpine")
    container.withCopyToContainer(text(Pki.ca.certPem), "/tls/ca.pem")
    container.withCopyToContainer(text(Pki.server.chainPem), "/tls/server.pem")
    container.withCopyToContainer(text(Pki.server.keyPem), "/tls/server-key.pem")
    container.withCommand(
      "redis-server", "--port", "0", "--tls-port", "6379", "--tls-cert-file", "/tls/server.pem", "--tls-key-file", "/tls/server-key.pem",
      "--tls-ca-cert-file", "/tls/ca.pem", "--tls-auth-clients", "yes"
    )
    container.withExposedPorts(6379)
    container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
    container.start()
    started(container)
  }

  lazy val mosquitto: Container = {
    val container = new Container("eclipse-mosquitto:2.0.21")
    container.withCopyToContainer(text(Pki.ca.certPem), "/mosquitto/config/ca.pem")
    container.withCopyToContainer(text(Pki.server.chainPem), "/mosquitto/config/server.pem")
    container.withCopyToContainer(text(Pki.server.keyPem), "/mosquitto/config/server-key.pem")
    container.withCopyToContainer(
      text(
        "listener 8883\ncafile /mosquitto/config/ca.pem\ncertfile /mosquitto/config/server.pem\nkeyfile /mosquitto/config/server-key.pem\n" +
          "require_certificate true\nallow_anonymous true\n"
      ),
      "/mosquitto/config/mosquitto.conf"
    )
    container.withCommand("mosquitto", "-c", "/mosquitto/config/mosquitto.conf")
    container.withExposedPorts(8883)
    container.waitingFor(Wait.forLogMessage(".*mosquitto version .* running.*", 1))
    container.start()
    started(container)
  }

  lazy val rabbitmq: RabbitMQContainer = {
    val container = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-management-alpine"))
    container.withCopyToContainer(text(Pki.ca.certPem), "/etc/rabbitmq/tls/ca.pem")
    container.withCopyToContainer(text(Pki.server.chainPem), "/etc/rabbitmq/tls/server.pem")
    container.withCopyToContainer(text(Pki.server.keyPem), "/etc/rabbitmq/tls/server-key.pem")
    // next to the configuration of the image, not instead of it
    container.withCopyToContainer(
      text(
        "listeners.ssl.default = 5671\nssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem\nssl_options.certfile = /etc/rabbitmq/tls/server.pem\n" +
          "ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem\nssl_options.verify = verify_peer\nssl_options.fail_if_no_peer_cert = true\n"
      ),
      "/etc/rabbitmq/conf.d/20-tls.conf"
    )
    container.start()
    started(container)
  }

  lazy val artemis: Container = {
    val container = new Container("apache/artemis:2.55.0-alpine")
    val etc       = "/var/lib/artemis-instance/etc"
    // copied over the configuration when the instance is created
    container.withCopyToContainer(binary(Pki.server.keyStore), "/var/lib/artemis-instance/etc-override/server.p12")
    container.withCopyToContainer(binary(Pki.ca.trustStore), "/var/lib/artemis-instance/etc-override/ca.p12")
    container.withCopyToContainer(
      text(
        Seq(
          "factoryClassName" -> "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory",
          "params.host"               -> "0.0.0.0",
          "params.port"               -> "61617",
          "params.protocols"          -> "CORE",
          "params.sslEnabled"         -> "true",
          "params.keyStorePath"       -> s"$etc/server.p12",
          "params.keyStorePassword"   -> Pki.Password,
          "params.keyStoreType"       -> "PKCS12",
          "params.trustStorePath"     -> s"$etc/ca.p12",
          "params.trustStorePassword" -> Pki.Password,
          "params.trustStoreType"     -> "PKCS12",
          "params.needClientAuth"     -> "true"
        ).map((k, v) => s"acceptorConfigurations.tls.$k=$v").mkString("\n")
      ),
      "/var/lib/artemis-instance/etc-override/broker.properties"
    )
    container.withExposedPorts(61617)
    container.waitingFor(Wait.forLogMessage(".*AMQ221007.*", 1))
    container.start()
    started(container)
  }

  /** kafka tells its clients where its brokers are: the port it advertises is the one of the host */
  lazy val kafkaPort: Int = freePort()

  lazy val kafka: FixedPortContainer = {
    val port      = kafkaPort
    val container = new FixedPortContainer("apache/kafka:3.8.0")
    container.withFixedExposedPort(port, port)
    Seq("server.p12" -> Pki.server.keyStore, "ca.p12" -> Pki.ca.trustStore).foreach((name, content) => container.withCopyToContainer(binary(content), s"/etc/kafka/secrets/$name"))
    container.withCopyToContainer(text(Pki.Password), "/etc/kafka/secrets/password")
    Map(
      "KAFKA_NODE_ID"                                  -> "1",
      "KAFKA_PROCESS_ROLES"                            -> "broker,controller",
      "KAFKA_LISTENERS"                                -> s"SSL://0.0.0.0:$port,PLAINTEXT://0.0.0.0:19092,CONTROLLER://0.0.0.0:19093",
      "KAFKA_ADVERTISED_LISTENERS"                     -> s"SSL://localhost:$port,PLAINTEXT://localhost:19092",
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"           -> "SSL:SSL,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
      "KAFKA_CONTROLLER_LISTENER_NAMES"                -> "CONTROLLER",
      "KAFKA_CONTROLLER_QUORUM_VOTERS"                 -> "1@localhost:19093",
      "KAFKA_INTER_BROKER_LISTENER_NAME"               -> "PLAINTEXT",
      "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"         -> "1",
      "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
      "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"            -> "1",
      "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"         -> "0",
      "KAFKA_SSL_KEYSTORE_FILENAME"                    -> "server.p12",
      "KAFKA_SSL_KEYSTORE_TYPE"                        -> "PKCS12",
      "KAFKA_SSL_KEYSTORE_CREDENTIALS"                 -> "password",
      "KAFKA_SSL_KEY_CREDENTIALS"                      -> "password",
      "KAFKA_SSL_TRUSTSTORE_FILENAME"                  -> "ca.p12",
      "KAFKA_SSL_TRUSTSTORE_TYPE"                      -> "PKCS12",
      "KAFKA_SSL_TRUSTSTORE_CREDENTIALS"               -> "password",
      "KAFKA_SSL_CLIENT_AUTH"                          -> "required",
      "CLUSTER_ID"                                     -> "4L6g3nShT-eMCtK--X86sw",
      "KAFKA_HEAP_OPTS"                                -> Brokers.KafkaMemory
    ).foreach((k, v) => container.withEnv(k, v))
    container.waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
    container.start()
    started(container)
  }

  lazy val pulsar: PulsarContainer = {
    val container = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.5"))
    container.withEnv("PULSAR_MEM", Brokers.PulsarMemory)
    container.withCopyToContainer(text(Pki.ca.certPem), "/pulsar/tls/ca.pem")
    container.withCopyToContainer(text(Pki.server.chainPem), "/pulsar/tls/server.pem")
    container.withCopyToContainer(text(Pki.server.keyPem), "/pulsar/tls/server-key.pem")
    Map(
      // not in the stock configuration: added, not replaced
      "PULSAR_PREFIX_brokerServicePortTls"   -> "6651",
      "PULSAR_PREFIX_webServicePortTls"      -> "8443",
      "tlsCertificateFilePath"               -> "/pulsar/tls/server.pem",
      "tlsKeyFilePath"                       -> "/pulsar/tls/server-key.pem",
      "tlsTrustCertsFilePath"                -> "/pulsar/tls/ca.pem",
      "tlsRequireTrustedClientCertOnConnect" -> "true"
    ).foreach((k, v) => container.withEnv(k, v))
    container.addExposedPort(6651)
    container.start()
    started(container)
  }
}
