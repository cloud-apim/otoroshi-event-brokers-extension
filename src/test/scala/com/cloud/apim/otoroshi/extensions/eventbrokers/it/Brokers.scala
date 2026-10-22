package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.pulsar.PulsarContainer
import org.testcontainers.rabbitmq.RabbitMQContainer
import org.testcontainers.utility.DockerImageName

final class Container(image: String) extends GenericContainer[Container](DockerImageName.parse(image))

/**
 * One container per broker, started the first time a suite asks for it and shared by every suite
 * after that. Nothing stops them explicitly: testcontainers' reaper removes them when the jvm exits.
 */
object Brokers {

  lazy val kafka: KafkaContainer = {
    val container = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
    container.start()
    container
  }

  lazy val rabbitmq: RabbitMQContainer = {
    val container = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-management-alpine"))
    container.start()
    container
  }

  lazy val pulsar: PulsarContainer = {
    val container = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.5"))
    container.start()
    container
  }

  lazy val redis: Container = {
    val container = new Container("redis:7.4-alpine")
    container.withExposedPorts(6379)
    container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
    container.start()
    container
  }

  lazy val mosquitto: Container = {
    val container = new Container("eclipse-mosquitto:2.0.21")
    // the stock configuration only listens on the loopback of the container
    container.withCommand("mosquitto", "-c", "/mosquitto-no-auth.conf")
    container.withExposedPorts(1883)
    container.waitingFor(Wait.forLogMessage(".*mosquitto version .* running.*", 1))
    container.start()
    container
  }

  def redisUri: String     = s"redis://${redis.getHost}:${redis.getMappedPort(6379)}"
  def mosquittoPort: Int   = mosquitto.getMappedPort(1883)
}
