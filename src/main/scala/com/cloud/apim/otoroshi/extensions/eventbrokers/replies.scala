package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.PluginUtils
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import otoroshi.env.Env
import play.api.libs.json.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

/**
 * Request-reply over a broker: a request goes out with the `correlationid` and `replyto` extensions
 * (and, where the broker has them, its own fields: the amqp `reply_to` and `correlation_id`, the mqtt 5
 * response topic); the service answering publishes an event with the same `correlationid` where
 * `replyto` says. Every node reads the replies (a subscription of its own, no group: each one gets
 * them all) and keeps those of the requests it sent.
 */
final class ReplyListener(connection: String, options: JsObject, env: Env, connections: ConnectionManager) {

  private val logger                 = play.api.Logger("cloud-apim-event-brokers-replies")
  private given ec: ExecutionContext = env.otoroshiExecutionContext
  private given Env                  = env

  private val pending = new TrieMap[String, Promise[CloudEvent]]()
  private val ready   = Promise[Unit]()
  /** where the replies are read from, on the brokers that can start from a date: since the listener is there, or since its subscription stopped */
  @volatile private var since = System.currentTimeMillis()

  private def correlation(received: ReceivedEvent, event: CloudEvent): Option[String] =
    event.extension("correlationid").orElse(received.message.metadata.get("correlation_id"))

  private def subscription(): Future[Source[Unit, ?]] =
    connections.acquire(connection) match {
      case Left(why)    => Future.failed(new IllegalStateException(why.message))
      case Right(lease) =>
        val client = lease.client
        // a broker that can start from a date misses nothing: not what comes while it subscribes, not what
        // came while it could not (a reply topic that does not exist yet, a broker away)
        val start  = if (ReplyListener.fromDate(client)) Json.obj("from_timestamp" -> since.toString) else Json.obj()
        PluginUtils.subscriptionSpec(start ++ options ++ Json.obj("delivery" -> "at-most-once")) match {
          case Left(why)   =>
            lease.release()
            Future.failed(new IllegalArgumentException(why))
          case Right(spec) =>
            Future
              .fromTry(Try(client.subscribe(spec)))
              .flatten
              .map { events =>
                ready.trySuccess(())
                lease
                  .releasing(events.map { received =>
                    val event = CloudEventBindings.fromMessage(received.message, client.config.kind)
                    correlation(received, event).flatMap(pending.remove).foreach(_.trySuccess(event))
                  })
                  .watchTermination() { (mat, done) =>
                    // the next subscription goes on from about here
                    done.onComplete(_ => since = System.currentTimeMillis() - 1000L)
                    mat
                  }
              }
              .andThen { case Failure(_) => lease.release() }
        }
    }

  private val switch: UniqueKillSwitch = {
    given Materializer = env.otoroshiMaterializer
    RestartSource
      .withBackoff(RestartSettings(500.millis, 10.seconds, 0.2)) { () =>
        Source.futureSource(subscription().andThen { case Failure(e) => logger.warn(s"could not read the replies of '$connection': ${ConnectionManager.describe(e)}") })
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.ignore)
      .run()
  }

  /**
   * once the replies are read, or `atMost`: on a broker that cannot start from a date, a request sent
   * before could have its reply lost
   */
  def whenReady(atMost: FiniteDuration): Future[Unit] =
    Future.firstCompletedOf(Seq(ready.future, org.apache.pekko.pattern.after(atMost)(Future.unit)(using env.otoroshiActorSystem)))

  /** what to wait for a reply with: completed by the reply, failed after `timeout` */
  def expect(correlationId: String, timeout: FiniteDuration): Future[CloudEvent] = {
    val promise = Promise[CloudEvent]()
    pending.put(correlationId, promise)
    val timer   = env.otoroshiScheduler.scheduleOnce(timeout) {
      pending.remove(correlationId)
      promise.tryFailure(new java.util.concurrent.TimeoutException(s"no reply within ${timeout.toMillis} ms"))
      ()
    }
    promise.future.andThen { case _ => timer.cancel() }
  }

  def forget(correlationId: String): Unit = pending.remove(correlationId)

  def stop(): Unit = {
    switch.shutdown()
    pending.values.foreach(_.tryFailure(new IllegalStateException("stopped")))
    pending.clear()
  }
}

object ReplyListener {
  /** whether the replies can be read from a date: not on rabbitmq, where only a stream replays */
  def fromDate(client: EventBrokerClient): Boolean =
    client.capabilities.replay && client.config.kind != com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnectionKind.RabbitMQ
}

/** the reply listeners of this node, one per connection and reply destination, made on first use */
final class ReplyListeners(env: Env, connections: ConnectionManager) {

  private val listeners = new TrieMap[String, ReplyListener]()

  def listener(connection: String, options: JsObject): ReplyListener = {
    val key = s"$connection ${Json.stringify(options)}"
    listeners.getOrElseUpdate(key, new ReplyListener(connection, options, env, connections))
  }

  def stop(): Unit = {
    listeners.values.foreach(l => Try(l.stop()))
    listeners.clear()
  }
}
