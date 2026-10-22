package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.ConnectionState
import io.lettuce.core.RedisClient
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.FixedHostPortGenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.*
import play.api.libs.json.*

import java.net.ServerSocket
import scala.concurrent.duration.*

// deprecated, but the plain way to have a broker appear on a port chosen before it starts
@annotation.nowarn("cat=deprecation")
final class FixedPortContainer(image: String) extends FixedHostPortGenericContainer[FixedPortContainer](image)

/**
 * The lifecycle of the connections against a real broker: one that is not there yet, one that stops
 * answering and comes back, settings that change, a connection that goes. Redis is the cheapest
 * broker to start, and every client opens exactly one connection to it, which redis can count.
 */
class ConnectionsSuite extends BrokerSuite {

  import Gateway.given

  // a redis of its own: this suite pauses it
  lazy val redis: Container = {
    val container = new Container("redis:7.4-alpine")
    container.withExposedPorts(6379)
    container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
    container.start()
    container
  }

  override def startBroker(): Unit = { redis; () }

  override def afterAll(): Unit = if (!munitIgnore) redis.stop()

  def redisUri: String = s"redis://${redis.getHost}:${redis.getMappedPort(6379)}"

  def freePort(): Int = {
    val socket = new ServerSocket(0)
    val port   = socket.getLocalPort
    socket.close()
    port
  }

  /** how many clients redis has, ours included */
  def connectedClients(): Int = {
    val client = RedisClient.create(redisUri)
    try {
      val connection = client.connect()
      try "connected_clients:(\\d+)".r.findFirstMatchIn(connection.sync().info("clients")).map(_.group(1).toInt).getOrElse(-1)
      finally connection.close()
    } finally client.shutdown()
  }

  def docker = DockerClientFactory.instance().client()

  def publish(route: otoroshi.next.models.NgRoute, n: Int): (Int, Seq[JsValue]) =
    Gateway.post(route, "/" + opts("channel" -> "lifecycle"), Json.stringify(message(n, "lifecycle")))

  def stateOf(id: String): Option[String] = (Gateway.connectionStatus(id).json \ "state").asOpt[String]

  test("an unreachable broker: reported unavailable, and its routes answer 503 at once") {
    val id        = Gateway.createConnection("redis", Json.obj("uris" -> Json.arr(s"redis://127.0.0.1:${freePort()}")), waitConnected = false)
    eventually("unavailable")(stateOf(id).contains("unavailable"))
    val status    = Gateway.connectionStatus(id).json
    assert((status \ "error").asOpt[String].exists(_.nonEmpty), status.toString)
    assert((status \ "next_attempt_at").asOpt[Long].isDefined, status.toString)
    assert((Gateway.connectionStatuses().json \ "connections").as[Seq[JsValue]].exists(c => (c \ "id").asOpt[String].contains(id)))
    val route     = Gateway.brokerRoute[HttpEventsConsumer](id)
    val startedAt = System.currentTimeMillis()
    val res       = await(Gateway.request(route, HttpMethods.GET, "/"))
    res.discardEntityBytes()
    assertEquals(res.status.intValue(), 503)
    assert(res.headers.exists(h => h.lowercaseName == "retry-after"), res.headers.toString)
    assert(System.currentTimeMillis() - startedAt < 2000L, "the route waited for the broker")
    eventually("the alert") {
      Gateway.eventsOf(id).exists(e => (e \ "alert").asOpt[String].contains("EventBrokerConnectionUnavailable"))
    }
    Gateway.deleteConnection(id)
  }

  test("comes up as soon as its broker does, without a request to trigger it") {
    val port      = freePort()
    val id        = Gateway.createConnection("redis", Json.obj("uris" -> Json.arr(s"redis://127.0.0.1:$port")), waitConnected = false)
    eventually("unavailable")(stateOf(id).contains("unavailable"))
    val late      = new FixedPortContainer("redis:7.4-alpine")
    late.withFixedExposedPort(port, 6379)
    late.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
    late.start()
    try {
      eventually("connected")(stateOf(id).contains("connected"))
      val ingester        = Gateway.brokerRoute[HttpEventsIngester](id)
      val (status, lines) = publish(ingester, 0)
      assertEquals(status, 200, lines.toString)
      eventually("the recovery event") {
        Gateway.eventsOf(id).exists(e => (e \ "previous_state").asOpt[String].contains("unavailable") && (e \ "state").asOpt[String].contains("connected"))
      }
    } finally {
      Gateway.deleteConnection(id)
      late.stop()
    }
  }

  test("a broker that stops answering: unavailable, 503, and back once it answers again") {
    val id       = Gateway.createConnection("redis", Json.obj("uris" -> Json.arr(redisUri)))
    val ingester = Gateway.brokerRoute[HttpEventsIngester](id)
    assertEquals(publish(ingester, 0)._1, 200)
    docker.pauseContainerCmd(redis.getContainerId).exec()
    try {
      eventually("unavailable", 20.seconds)(stateOf(id).contains("unavailable"))
      assertEquals(publish(ingester, 1)._1, 503)
    } finally docker.unpauseContainerCmd(redis.getContainerId).exec()
    eventually("connected again", 30.seconds)(stateOf(id).contains("connected"))
    val (status, lines) = publish(ingester, 2)
    assertEquals(status, 200, lines.toString)
    Gateway.deleteConnection(id)
  }

  test("new settings take over with no gap, the previous client is closed, and deleting closes the last one") {
    // the probe that counts is a client too, the connections of the tests above are all deleted
    val before   = 1
    eventually("only the probe connected")(connectedClients() == before)
    val id       = Gateway.createConnection("redis", Json.obj("uris" -> Json.arr(redisUri), "channel" -> "first"))
    eventually("one more client")(connectedClients() == before + 1)
    val ingester = Gateway.brokerRoute[HttpEventsIngester](id)
    val handle   = Gateway.extension.connections.handles(id).head
    Gateway.updateConnection(id, "redis", Json.obj("uris" -> Json.arr(redisUri), "channel" -> "second"))
    // publish all along the switch: never refused
    var n        = 0
    val deadline = System.currentTimeMillis() + 15000L
    while (!handle.isRetired && System.currentTimeMillis() < deadline) {
      val (status, lines) = publish(ingester, n)
      assertEquals(status, 200, s"publish $n refused during the switch: $lines")
      n = n + 1
      Thread.sleep(50L)
    }
    assert(handle.isRetired, "the new settings never took over")
    assertEquals(Gateway.connectionState(id), Some(ConnectionState.Connected))
    eventually("the previous client closed")(connectedClients() == before + 1)
    Gateway.deleteConnection(id)
    eventually("the last client closed")(connectedClients() == before)
    eventually("gone from the status api")(Gateway.connectionStatus(id).status == 404)
    val (status, lines) = publish(ingester, n)
    assertEquals(status, 500, lines.toString)
    assertEquals((lines.head \ "error").asOpt[String], Some("connection_not_found"))
  }
}
