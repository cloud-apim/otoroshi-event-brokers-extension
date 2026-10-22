package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.typesafe.config.ConfigFactory
import play.api.Configuration

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

class ConnectionManagerSettingsSuite extends munit.FunSuite {

  val settings = ConnectionManagerSettings.default.copy(minBackoff = 1.second, maxBackoff = 30.seconds)

  test("the backoff doubles from the minimum, with at most 20% of jitter") {
    (1 to 5).foreach { failures =>
      val base  = 1000L * math.pow(2, failures - 1).toLong
      val delay = settings.backoff(failures).toMillis
      assert(delay >= base && delay <= (base * 1.2).toLong, s"$failures failures: $delay ms")
    }
  }

  test("the backoff stops growing at the maximum") {
    (6 to 40).foreach { failures =>
      val delay = settings.backoff(failures).toMillis
      assert(delay >= 30000L && delay <= 36000L, s"$failures failures: $delay ms")
    }
  }

  test("reads the connections block of the extension configuration") {
    val read = ConnectionManagerSettings.from(
      Configuration(
        ConfigFactory.parseString("""
          |connections {
          |  connect-timeout = 3s
          |  retry.min-backoff = 200ms
          |  retry.max-backoff = 5s
          |  healthcheck.interval = 2s
          |  healthcheck.timeout = 1s
          |  healthcheck.max-failures = 7
          |  retire-grace-period = 1m
          |  close-timeout = 4s
          |}
          |""".stripMargin)
      )
    )
    assertEquals(read, ConnectionManagerSettings(3.seconds, 200.millis, 5.seconds, 2.seconds, 1.second, 7, 1.minute, 4.seconds))
  }

  test("defaults for what the configuration does not say") {
    assertEquals(ConnectionManagerSettings.from(Configuration.empty), ConnectionManagerSettings.default)
  }

  test("an error is described by its innermost message") {
    val e = new RuntimeException("wrapper", new IllegalStateException("broker said no"))
    assertEquals(ConnectionManager.describe(e), "IllegalStateException: broker said no")
    assertEquals(ConnectionManager.describe(new TimeoutException("connecting timed out")), "TimeoutException: connecting timed out")
    assertEquals(ConnectionManager.describe(new RuntimeException()), "RuntimeException")
  }
}
