package com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.stream.scaladsl.Source
import otoroshi.env.Env
import play.api.libs.json.*

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** what went through a connection, or a route, on this node since it started */
final class TrafficCounters {
  val published       = new AtomicLong(0L)
  val publishFailed   = new AtomicLong(0L)
  val consumed        = new AtomicLong(0L)
  val acked           = new AtomicLong(0L)
  val subscriptions   = new AtomicLong(0L)
  private val batches = new AtomicLong(0L)
  private val millis  = new AtomicLong(0L)
  private val slowest = new AtomicLong(0L)

  def publishedBatch(ok: Long, failed: Long, took: Long): Unit = {
    published.addAndGet(ok)
    publishFailed.addAndGet(failed)
    batches.incrementAndGet()
    millis.addAndGet(took)
    slowest.accumulateAndGet(took, math.max)
  }

  def json: JsObject = Json.obj(
    "published"      -> published.get(),
    "publish_failed" -> publishFailed.get(),
    "consumed"       -> consumed.get(),
    "acked"          -> acked.get(),
    "subscriptions"  -> subscriptions.get(),
    // how long the broker took to take a batch of events
    "publish_ms"     -> Json.obj("mean" -> (if (batches.get() == 0) 0.0 else millis.get().toDouble / batches.get()), "max" -> slowest.get())
  )
}

/**
 * The traffic of this node, by connection and by route: kept in memory for the status api, and sent
 * to otoroshi's metrics (`cloud-apim.event-brokers.connections.<id>.*`, `cloud-apim.event-brokers.routes.<id>.*`).
 */
final class Traffic(env: Env) {

  private val connections = new TrieMap[String, TrafficCounters]()
  private val routes      = new TrieMap[String, TrafficCounters]()

  private def of(connection: String, route: Option[String]): Seq[(String, TrafficCounters)] =
    Seq(s"cloud-apim.event-brokers.connections.$connection" -> connections.getOrElseUpdate(connection, new TrafficCounters())) ++
      route.map(r => s"cloud-apim.event-brokers.routes.$r" -> routes.getOrElseUpdate(r, new TrafficCounters()))

  private def metrics(f: => Unit): Unit = if (env.metricsEnabled) Try(f)

  def published(connection: String, route: Option[String], ok: Long, failed: Long, took: Long): Unit =
    of(connection, route).foreach { (name, counters) =>
      counters.publishedBatch(ok, failed, took)
      metrics {
        if (ok > 0) env.metrics.counterIncOf(s"$name.published", ok)
        if (failed > 0) env.metrics.counterIncOf(s"$name.publish_failed", failed)
        env.metrics.timerUpdate(s"$name.publish", took, TimeUnit.MILLISECONDS)
      }
    }

  def consumed(connection: String, route: Option[String]): Unit = of(connection, route).foreach { (name, counters) =>
    counters.consumed.incrementAndGet()
    metrics(env.metrics.counterInc(s"$name.consumed"))
  }

  def acked(connection: String, route: Option[String]): Unit = of(connection, route).foreach { (name, counters) =>
    counters.acked.incrementAndGet()
    metrics(env.metrics.counterInc(s"$name.acked"))
  }

  private def subscriptions(connection: String, route: Option[String], delta: Long): Unit = of(connection, route).foreach { (name, counters) =>
    val now = counters.subscriptions.addAndGet(delta)
    metrics(env.metrics.markLong(s"$name.subscriptions", now))
  }

  /** counts what a subscription delivers and acks, and that it is open while it streams */
  def watch(client: EventBrokerClient, route: Option[String])(source: Source[ReceivedEvent, ?]): Source[ReceivedEvent, ?] = {
    val connection = client.config.id
    source
      .map { received =>
        consumed(connection, route)
        new ReceivedEvent(received.message, () => received.ack().map(_ => acked(connection, route))(using ExecutionContext.parasitic))
      }
      .mapMaterializedValue { mat =>
        subscriptions(connection, route, 1L)
        mat
      }
      .watchTermination() { (mat, done) =>
        done.onComplete(_ => subscriptions(connection, route, -1L))(using ExecutionContext.parasitic)
        mat
      }
  }

  /** counts a batch published: one result per event */
  def publishing(client: EventBrokerClient, route: Option[String])(batch: => Future[Seq[PublishResult]])(using ec: ExecutionContext): Future[Seq[PublishResult]] = {
    val start = System.currentTimeMillis()
    batch.andThen { case scala.util.Success(results) =>
      published(client.config.id, route, results.count(_.published).toLong, results.count(!_.published).toLong, System.currentTimeMillis() - start)
    }
  }

  def connectionJson(id: String): JsObject = connections.get(id).map(_.json).getOrElse(new TrafficCounters().json)

  def json: JsObject = Json.obj(
    "connections" -> JsObject(connections.toSeq.sortBy(_._1).map((id, c) => id -> c.json)),
    "routes"      -> JsObject(routes.toSeq.sortBy(_._1).map((id, c) => id -> c.json))
  )
}

object Traffic {
  /** the traffic of the extension, when it runs */
  def of(env: Env): Option[Traffic] =
    env.adminExtensions.extension[otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension].map(_.traffic)
}
