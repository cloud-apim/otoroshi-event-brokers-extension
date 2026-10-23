package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** the brokers of the authentication tests, each one refusing whoever it does not know */
object AuthBrokers {

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

  private def text(content: String): Transferable = Transferable.of(content.getBytes(StandardCharsets.UTF_8), 0x1a4)

  // ---------------------------------------------------------------------------------------------
  // kafka: scram-sha-512 (alice) and plain (bob) on a sasl_plaintext listener
  // ---------------------------------------------------------------------------------------------

  lazy val kafkaPort: Int = DockerSupport.freePort()

  lazy val kafka: FixedPortContainer = {
    val port       = kafkaPort
    val container  = new FixedPortContainer("apache/kafka:3.8.0")
    container.withFixedExposedPort(port, port)
    val properties = Seq(
      "process.roles"                                            -> "broker,controller",
      "node.id"                                                  -> "1",
      "controller.quorum.voters"                                 -> "1@localhost:19093",
      "listeners"                                                -> s"SASL_PLAINTEXT://0.0.0.0:$port,PLAINTEXT://0.0.0.0:19092,CONTROLLER://0.0.0.0:19093",
      "advertised.listeners"                                     -> s"SASL_PLAINTEXT://localhost:$port,PLAINTEXT://localhost:19092",
      "listener.security.protocol.map"                           -> "SASL_PLAINTEXT:SASL_PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
      "controller.listener.names"                                -> "CONTROLLER",
      "inter.broker.listener.name"                               -> "PLAINTEXT",
      "sasl.enabled.mechanisms"                                  -> "SCRAM-SHA-512,PLAIN",
      "listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config" -> "org.apache.kafka.common.security.scram.ScramLoginModule required;",
      "listener.name.sasl_plaintext.plain.sasl.jaas.config"      -> "org.apache.kafka.common.security.plain.PlainLoginModule required user_bob=\"bob-secret\";",
      "offsets.topic.replication.factor"                         -> "1",
      "transaction.state.log.replication.factor"                 -> "1",
      "transaction.state.log.min.isr"                            -> "1",
      "group.initial.rebalance.delay.ms"                         -> "0",
      "log.dirs"                                                 -> "/tmp/kraft-logs"
    ).map((k, v) => s"$k=$v").mkString("\n")
    container.withCopyToContainer(text(properties), "/tmp/sasl.properties")
    // the scram credentials are written with the storage, before the broker starts
    container.withCommand(
      "bash",
      "-c",
      "/opt/kafka/bin/kafka-storage.sh format -t 4L6g3nShT-eMCtK--X86sw -c /tmp/sasl.properties --add-scram 'SCRAM-SHA-512=[name=alice,password=alice-secret]' && exec /opt/kafka/bin/kafka-server-start.sh /tmp/sasl.properties"
    )
    container.waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
    container.start()
    started(container)
  }

  // ---------------------------------------------------------------------------------------------
  // pulsar: jwt tokens signed with a secret key
  // ---------------------------------------------------------------------------------------------

  private val secret = { val bytes = new Array[Byte](32); new SecureRandom().nextBytes(bytes); bytes }

  private def b64(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  /** an hs256 token for `subject`, signed with `key` (the broker's by default) */
  def token(subject: String, key: Array[Byte] = secret): String = {
    val content = s"${b64("""{"alg":"HS256"}""".getBytes(StandardCharsets.UTF_8))}.${b64(s"""{"sub":"$subject"}""".getBytes(StandardCharsets.UTF_8))}"
    val mac     = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key, "HmacSHA256"))
    s"$content.${b64(mac.doFinal(content.getBytes(StandardCharsets.UTF_8)))}"
  }

  lazy val pulsar: Container = {
    val container = new Container("apachepulsar/pulsar:4.0.5")
    Map(
      "authenticationEnabled"               -> "true",
      "authenticationProviders"             -> "org.apache.pulsar.broker.authentication.AuthenticationProviderToken",
      "tokenSecretKey"                      -> s"data:;base64,${Base64.getEncoder.encodeToString(secret)}",
      "superUserRoles"                      -> "admin",
      // what the broker uses to talk to itself
      "brokerClientAuthenticationPlugin"    -> "org.apache.pulsar.client.impl.auth.AuthenticationToken",
      "brokerClientAuthenticationParameters" -> s"token:${token("admin")}",
      "PULSAR_PREFIX_clusterName"           -> "standalone"
    ).foreach((k, v) => container.withEnv(k, v))
    container.withCommand("/bin/bash", "-c", "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf && bin/pulsar standalone --no-functions-worker -nss")
    container.withExposedPorts(6650)
    container.waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1).withStartupTimeout(java.time.Duration.ofMinutes(3)))
    container.start()
    started(container)
  }

  // ---------------------------------------------------------------------------------------------
  // redis: an acl user, the default user turned off
  // ---------------------------------------------------------------------------------------------

  lazy val redis: Container = {
    val container = new Container("redis:7.4-alpine")
    container.withCommand("redis-server", "--user", "default", "off", "--user", "alice", "on", ">alice-secret", "~*", "&*", "+@all")
    container.withExposedPorts(6379)
    container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
    container.start()
    started(container)
  }

  // ---------------------------------------------------------------------------------------------
  // mosquitto: a password file
  // ---------------------------------------------------------------------------------------------

  lazy val mosquitto: Container = {
    val container = new Container("eclipse-mosquitto:2.0.21")
    container.withCopyToContainer(text("listener 1883\nallow_anonymous false\npassword_file /mosquitto/config/passwd\n"), "/mosquitto/config/mosquitto.conf")
    container.withCommand(
      "sh",
      "-c",
      "mosquitto_passwd -b -c /mosquitto/config/passwd alice alice-secret && chown mosquitto:mosquitto /mosquitto/config/passwd && chmod 0600 /mosquitto/config/passwd && exec mosquitto -c /mosquitto/config/mosquitto.conf"
    )
    container.withExposedPorts(1883)
    container.waitingFor(Wait.forLogMessage(".*mosquitto version .* running.*", 1))
    container.start()
    started(container)
  }
}
