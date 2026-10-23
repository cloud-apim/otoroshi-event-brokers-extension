package com.cloud.apim.otoroshi.extensions.eventbrokers.it

/**
 * The broker suites run real brokers in containers. They skip themselves when no docker daemon is
 * reachable, so `sbt test` stays runnable without one (the unit suites still run).
 */
object DockerSupport {

  lazy val available: Boolean =
    try org.testcontainers.DockerClientFactory.instance().isDockerAvailable
    catch { case _: Throwable => false }

  /**
   * a port for a container bound to a fixed port of the host. not one of the ephemeral range, where
   * docker picks the host ports of the other containers: a port free now could be taken a moment later
   */
  def freePort(): Int = {
    val random = new java.util.Random()
    Iterator
      .continually(20000 + random.nextInt(10000))
      .find(port => scala.util.Try(new java.net.ServerSocket(port).close()).isSuccess)
      .get
  }
}
