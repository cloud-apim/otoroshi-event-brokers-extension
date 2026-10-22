package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import play.api.libs.json.*

import java.util.Base64
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
 * Base of the suites that drive a real broker through the gateway.
 *
 * Tests named `.fail` pin a known defect of the current implementation: they pass as long as the
 * defect is there, and start failing the day it is fixed — then the marker goes.
 */
abstract class BrokerSuite extends munit.FunSuite {

  override val munitTimeout: Duration = 5.minutes

  override def munitIgnore: Boolean = !DockerSupport.available

  /** starts the broker container this suite needs */
  def startBroker(): Unit

  override def beforeAll(): Unit = {
    startBroker()
    Gateway.instance
    ()
  }

  export Gateway.{await, uniq, eventually}
  import Gateway.given

  /** the consumers' options travel in the query: `opts("topic" -> "t")` */
  def opts(values: (String, Any)*): String =
    values.map((k, v) => s"x-ca-event-broker-opts-$k=${java.net.URLEncoder.encode(v.toString, "UTF-8")}").mkString("?", "&", "")

  /** what was published, whatever the content type the event came back with */
  def payload(event: JsValue): JsValue = {
    val value = event \ "value"
    val data  = value \ "data"
    (value \ "type").asOpt[String].map(_.toLowerCase) match {
      case Some("json" | "application/json") => data.as[JsValue]
      case Some("text" | "text/plain")       => Json.parse(data.as[String])
      case _                                 => Json.parse(new String(Base64.getDecoder.decode(data.as[String]), "UTF-8"))
    }
  }

  def message(n: Int, run: String): JsObject = Json.obj("n" -> n, "run" -> run)

  def numbers(events: Seq[JsValue]): Seq[Int] = events.map(e => (payload(e) \ "n").as[Int])

  /** runs a stream in the background and collects at most `take` elements, or what came within `within` */
  def collect[A](source: Source[A, ?], take: Int, within: FiniteDuration = 20.seconds): Future[Seq[A]] =
    source.take(take.toLong).takeWithin(within).runWith(Sink.seq)

  /**
   * Pub/sub brokers only deliver what is published once the subscription is in place, and the
   * gateway gives no signal for that. So keep publishing until the consumer has what it wants.
   */
  def keepPublishing(until: Future[?], every: FiniteDuration = 250.millis, atMost: FiniteDuration = 30.seconds)(publish: Int => Unit): Int = {
    val deadline = System.currentTimeMillis() + atMost.toMillis
    var n        = 0
    while (!until.isCompleted && System.currentTimeMillis() < deadline) {
      publish(n)
      n = n + 1
      Thread.sleep(every.toMillis)
    }
    n
  }

  /** what a consumer got from a publishing loop: consecutive numbers, wherever it caught up */
  def assertConsecutive(ns: Seq[Int], expected: Int): Unit = {
    assertEquals(ns.size, expected, s"received $ns")
    assertEquals(ns, (ns.head until ns.head + expected).toSeq, s"received $ns")
  }
}
