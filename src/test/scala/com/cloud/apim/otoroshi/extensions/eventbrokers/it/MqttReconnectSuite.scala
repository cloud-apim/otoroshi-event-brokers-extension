package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.SubscriptionSpec
import com.cloud.apim.otoroshi.extensions.eventbrokers.clients.MQTTBrokerClient
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, EventBrokerConnectionKind, MqttSettings}
import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import org.apache.pekko.stream.scaladsl.Sink
import org.testcontainers.containers.wait.strategy.Wait
import otoroshi.env.Env
import otoroshi.models.EntityLocation

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.Try

/**
 * A clean mqtt session loses its subscriptions with its connection. The client, driven directly (the
 * manager would rebuild it after a few failed healthchecks, which is another story), must subscribe
 * again once it is connected again.
 */
class MqttReconnectSuite extends munit.FunSuite {

  override val munitTimeout: Duration = 3.minutes

  override def munitIgnore: Boolean = !DockerSupport.available

  import Gateway.{await, eventually, uniq}

  def freePort(): Int = DockerSupport.freePort()

  def mosquitto(port: Int): FixedPortContainer = {
    val container = new FixedPortContainer("eclipse-mosquitto:2.0.21")
    container.withFixedExposedPort(port, 1883)
    container.withCommand("mosquitto", "-c", "/mosquitto-no-auth.conf")
    container.waitingFor(Wait.forLogMessage(".*mosquitto version .* running.*", 1))
    container.start()
    container
  }

  def publish(host: String, port: Int, topic: String, text: String): Unit = Try {
    val client = MqttClient.builder().useMqttVersion5().identifier(uniq("native")).serverHost(host).serverPort(port).buildBlocking()
    client.connect()
    try client.publish(Mqtt5Publish.builder().topic(topic).payload(text.getBytes("UTF-8")).build())
    finally client.disconnect()
  }

  test("once the broker is back, the subscriptions are made again") {
    given Env              = Gateway.instance.env
    given ExecutionContext = ExecutionContext.global
    val port               = freePort()
    var broker             = mosquitto(port)
    val host               = broker.getHost
    val settings           = MqttSettings(host = host, port = port, keepAliveSeconds = 2)
    val connection         = EventBrokerConnection(EntityLocation.default, "c-reconnect", "reconnect", "", Seq.empty, Map.empty, EventBrokerConnectionKind.MQTT, settings.json)
    val client             = new MQTTBrokerClient(connection, settings)
    try {
      await(client.start())
      val received = new LinkedBlockingQueue[String]()
      await(client.subscribe(SubscriptionSpec(topics = Seq("reconnect"))))
        .runWith(Sink.foreach(e => received.add(e.message.data.utf8String)))(using Gateway.mat)
      eventually("delivered before")({ publish(host, port, "reconnect", "before"); Option(received.poll(200, TimeUnit.MILLISECONDS)).contains("before") })
      broker.stop()
      eventually("disconnected")(Try(await(client.healthcheck(), 1.second)).isFailure)
      broker = mosquitto(port)
      eventually("connected again", 60.seconds)(Try(await(client.healthcheck(), 1.second)).isSuccess)
      eventually("delivered after", 30.seconds)({ publish(host, port, "reconnect", "after"); Option(received.poll(200, TimeUnit.MILLISECONDS)).contains("after") })
    } finally {
      await(client.close())
      broker.stop()
    }
  }
}
