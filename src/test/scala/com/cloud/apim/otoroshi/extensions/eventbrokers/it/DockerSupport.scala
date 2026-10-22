package com.cloud.apim.otoroshi.extensions.eventbrokers.it

/**
 * The broker suites run real brokers in containers. They skip themselves when no docker daemon is
 * reachable, so `sbt test` stays runnable without one (the unit suites still run).
 */
object DockerSupport {

  lazy val available: Boolean =
    try org.testcontainers.DockerClientFactory.instance().isDockerAvailable
    catch { case _: Throwable => false }
}
