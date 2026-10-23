package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, EventBrokerWebhook}
import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.PluginUtils
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.statefulclients.LettuceStatefulClientConfig
import play.api.Configuration
import play.api.libs.json.*
import play.api.libs.ws.{BodyWritable, InMemoryBody}

import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

final case class WebhookManagerSettings(
    enabled: Boolean = true,
    /** the redis the nodes of a cluster share to decide which one delivers each webhook */
    redisUri: Option[String] = None,
    /** how long a node keeps a webhook without saying it still has it */
    lockTtl: FiniteDuration = 15.seconds,
    /** the kinds of nodes that deliver, in a cluster: leader, worker */
    nodeKinds: Seq[String] = Seq("leader", "worker")
)

object WebhookManagerSettings {
  def from(configuration: Configuration): WebhookManagerSettings = {
    val default = WebhookManagerSettings()
    WebhookManagerSettings(
      enabled = configuration.getOptional[Boolean]("webhooks.enabled").getOrElse(default.enabled),
      redisUri = configuration.getOptional[String]("webhooks.redis.uri").map(_.trim).filter(_.nonEmpty),
      lockTtl = configuration.getOptional[FiniteDuration]("webhooks.lock-ttl").getOrElse(default.lockTtl),
      nodeKinds = configuration.getOptional[Seq[String]]("webhooks.node-kinds").getOrElse(default.nodeKinds).map(_.toLowerCase)
    )
  }
}

/** which node delivers a webhook: one at a time, the one holding its lock */
trait WebhookLocks {
  /** takes the lock, or keeps it: whether this node has it now */
  def acquire(webhook: String): Future[Boolean]
  def release(webhook: String): Future[Unit]
  /** the node holding it, if any */
  def owner(webhook: String): Future[Option[String]]
  def json: JsObject
}

/** a single otoroshi: it delivers everything */
final class LocalWebhookLocks(node: String) extends WebhookLocks {
  def acquire(webhook: String): Future[Boolean]       = Future.successful(true)
  def release(webhook: String): Future[Unit]          = Future.unit
  def owner(webhook: String): Future[Option[String]]  = Future.successful(Some(node))
  def json: JsObject                                  = Json.obj("kind" -> "local")
}

/** a cluster without a shared redis: no node may deliver, two would deliver everything twice */
final class NoWebhookLocks(reason: String) extends WebhookLocks {
  def acquire(webhook: String): Future[Boolean]      = Future.successful(false)
  def release(webhook: String): Future[Unit]         = Future.unit
  def owner(webhook: String): Future[Option[String]] = Future.successful(None)
  def json: JsObject                                 = Json.obj("kind" -> "none", "error" -> reason)
}

/** locks in a redis shared by the nodes: a key per webhook holding the node, that expires unless renewed */
final class RedisWebhookLocks(node: String, connection: () => StatefulRedisConnection[String, ByteString], ttl: FiniteDuration, prefix: String)(using
    ec: ExecutionContext
) extends WebhookLocks {

  private def key(webhook: String): String = s"$prefix:webhooks:$webhook:owner"

  // renewed if this node has it, taken if nobody has it
  private val acquireScript =
    """if redis.call('get', KEYS[1]) == ARGV[1] then
      |  redis.call('pexpire', KEYS[1], ARGV[2])
      |  return 1
      |elseif redis.call('set', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
      |  return 1
      |else
      |  return 0
      |end""".stripMargin

  private val releaseScript =
    """if redis.call('get', KEYS[1]) == ARGV[1] then
      |  return redis.call('del', KEYS[1])
      |else
      |  return 0
      |end""".stripMargin

  def acquire(webhook: String): Future[Boolean] =
    connection()
      .async()
      .eval[java.lang.Long](acquireScript, ScriptOutputType.INTEGER, Array(key(webhook)), ByteString(node), ByteString(ttl.toMillis.toString))
      .asScala
      .map(_.longValue() == 1L)

  def release(webhook: String): Future[Unit] =
    connection().async().eval[java.lang.Long](releaseScript, ScriptOutputType.INTEGER, Array(key(webhook)), ByteString(node)).asScala.map(_ => ())

  def owner(webhook: String): Future[Option[String]] =
    connection().async().get(key(webhook)).asScala.map(v => Option(v).map(_.utf8String))

  def json: JsObject = Json.obj("kind" -> "redis")
}

object WebhookLocks {
  /** the locks for this node: local alone, in the shared redis in a cluster, none in a cluster without one */
  def apply(env: Env, settings: WebhookManagerSettings, node: String): WebhookLocks = {
    given ExecutionContext = env.otoroshiExecutionContext
    if (env.clusterConfig.mode.isOff) new LocalWebhookLocks(node)
    else if (!settings.nodeKinds.contains(env.clusterConfig.mode.name.toLowerCase)) new NoWebhookLocks(s"${env.clusterConfig.mode.name.toLowerCase} nodes do not deliver webhooks (webhooks.node-kinds)")
    else
      settings.redisUri match {
        case None      => new NoWebhookLocks("in a cluster, webhooks need a redis shared by the nodes: webhooks.redis.uri")
        case Some(uri) =>
          new RedisWebhookLocks(
            node,
            () => env.statefulClientsManager.client("cloud-apim-event-brokers-webhooks", LettuceStatefulClientConfig(uri)),
            settings.lockTtl,
            "cloud-apim:event-brokers"
          )
      }
  }
}

/** what came of one http delivery */
private[eventbrokers] enum DeliveryOutcome {
  case Delivered(status: Int)
  /** the endpoint said no for good: a 4xx other than 408 and 429 */
  case Refused(status: Int, body: String)
  /** worth trying again: a 408, 429 or 5xx, or no answer. `retryAfter`: what the endpoint asked for */
  case Failed(reason: String, retryAfter: Option[FiniteDuration])
}

/** an event as an http request, in the mode of the endpoint */
object WebhookRequest {

  private def signature(secret: String, body: ByteString): String = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))
    "sha256=" + mac.doFinal(body.toArray).map(b => f"$b%02x").mkString
  }

  /** the headers and the body of the request */
  def build(webhook: EventBrokerWebhook, event: CloudEvent, attempt: Int): (Seq[(String, String)], ByteString, String) = {
    val data              = event.data.getOrElse(ByteString.empty)
    val (headers, body, contentType) = webhook.endpoint.mode match {
      case "binary" =>
        val attributes = Seq("specversion" -> CloudEvent.SpecVersion, "id" -> event.id, "source" -> event.source, "type" -> event.`type`) ++
          event.subject.map("subject" -> _) ++ event.time.map("time" -> _) ++ event.dataschema.map("dataschema" -> _) ++
          event.extensions.keys.toSeq.sorted.flatMap(name => event.extension(name).map(name -> _))
        (attributes.map((name, value) => s"ce-$name" -> value), data, event.datacontenttype.getOrElse("application/octet-stream"))
      case "raw"    => (Seq.empty, data, event.datacontenttype.getOrElse("application/octet-stream"))
      case _        => (Seq.empty, ByteString(Json.stringify(event.json)), CloudEvent.ContentType)
    }
    val own                           = Seq("X-Event-Brokers-Webhook" -> webhook.id, "X-Event-Brokers-Attempt" -> attempt.toString) ++
      webhook.endpoint.secret.map(secret => "X-Event-Brokers-Signature" -> signature(secret, body))
    (webhook.endpoint.headers.toSeq ++ headers ++ own, body, contentType)
  }

  def send(webhook: EventBrokerWebhook, event: CloudEvent, attempt: Int)(using env: Env, ec: ExecutionContext): Future[DeliveryOutcome] = {
    val (headers, body, contentType) = build(webhook, event, attempt)
    val writable                     = BodyWritable[ByteString](b => InMemoryBody(b), contentType)
    val request                      =
      (if (webhook.endpoint.tls.enabled) env.MtlsWs.url(webhook.endpoint.url, webhook.endpoint.tls.mtlsConfig) else env.Ws.url(webhook.endpoint.url))
        .withMethod(webhook.endpoint.method)
        .withRequestTimeout(webhook.endpoint.timeoutMs.millis)
        .withHttpHeaders(headers*)
        .withBody(body)(using writable)
    request
      .execute()
      .map { res =>
        val retryAfter = res.header("Retry-After").flatMap(_.trim.toLongOption).map(_.seconds)
        res.status match {
          case s if s >= 200 && s < 300           => DeliveryOutcome.Delivered(s)
          case s if s == 408 || s == 429 || s >= 500 => DeliveryOutcome.Failed(s"the endpoint answered $s", retryAfter)
          case s                                   => DeliveryOutcome.Refused(s, res.body[String](using play.api.libs.ws.WSBodyReadables.readableAsString).take(500))
        }
      }
      .recover { case NonFatal(e) => DeliveryOutcome.Failed(ConnectionManager.describe(e), None) }
  }
}

/**
 * Delivers one webhook on this node while this node holds its lock: subscribes through the
 * connection manager, posts each event, acks it once delivered (or dead-lettered, or given up on),
 * and subscribes again with backoff when the subscription ends or fails.
 */
final class WebhookRunner(val webhook: EventBrokerWebhook, env: Env, connections: ConnectionManager, locks: WebhookLocks, renewEvery: FiniteDuration) {

  private val logger                = play.api.Logger("cloud-apim-event-brokers-webhooks")
  private given ec: ExecutionContext = env.otoroshiExecutionContext
  private given Env                 = env

  val delivered    = new AtomicLong(0L)
  val failed       = new AtomicLong(0L)
  val retried      = new AtomicLong(0L)
  val deadLettered = new AtomicLong(0L)
  private val lastError    = new AtomicReference[Option[(Long, String)]](None)
  private val lastDelivery = new AtomicReference[Option[Long]](None)
  private val running      = new AtomicReference[Option[UniqueKillSwitch]](None)
  @volatile private var stopped = false

  def hash: String = webhook.hash

  private def error(message: String): Unit = {
    lastError.set(Some((System.currentTimeMillis(), message)))
    logger.warn(s"webhook '${webhook.name}' (${webhook.id}): $message")
  }

  private val ticker = env.otoroshiScheduler.scheduleWithFixedDelay(0.seconds, renewEvery)(() => tick())

  private def tick(): Unit = if (!stopped) {
    locks.acquire(webhook.id).andThen {
      case Success(true)  => synchronized(if (!stopped && running.get().isEmpty) startDelivering())
      case Success(false) => stopDelivering()
      // not sure to hold it any more: another node may take it
      case Failure(e)     =>
        error(s"could not renew its lock: ${ConnectionManager.describe(e)}")
        stopDelivering()
    }
    ()
  }

  def isDelivering: Boolean = running.get().isDefined

  private def metric(name: String): Unit = Try(env.metrics.counterInc(s"cloud-apim.event-brokers.webhooks.${webhook.id}.$name"))

  /** the options of the subscription, with what a webhook needs by default */
  private def options(connection: EventBrokerConnection, client: EventBrokerClient): JsObject = {
    val asked    = webhook.subscription
    val anycast  = asked.value.contains("queue") || (asked \ "routing_type").asOpt[String].contains("anycast")
    // a group of its own: the node taking the webhook over goes on where the previous one stopped
    val group    =
      if (asked.value.contains("group") || !client.capabilities.groups || (connection.kind.name == "artemis" && anycast)) Json.obj()
      else Json.obj("group" -> s"otoroshi-webhook-${webhook.id}")
    val delivery =
      if (asked.value.contains("delivery")) Json.obj()
      else Json.obj("delivery" -> (if (client.capabilities.acks) "at-least-once" else "at-most-once"))
    asked ++ group ++ delivery
  }

  private def backoff(attempt: Int): FiniteDuration = {
    val base = math.min(webhook.retry.maxBackoffMs.toDouble, webhook.retry.initialBackoffMs * math.pow(2, attempt - 1)).toLong
    (base + (base * 0.2 * Random.nextDouble()).toLong).millis
  }

  /** posts an event, tries again as long as it may, and deals with it for good either way */
  private def deliver(event: CloudEvent): Future[Unit] = {
    def attempt(n: Int): Future[Unit] =
      WebhookRequest.send(webhook, event, n).flatMap {
        case DeliveryOutcome.Delivered(_)                                      =>
          delivered.incrementAndGet()
          lastDelivery.set(Some(System.currentTimeMillis()))
          metric("delivered")
          Future.unit
        case DeliveryOutcome.Refused(status, body)                              => givenUp(event, n, s"the endpoint refused it with $status: $body")
        case DeliveryOutcome.Failed(reason, _) if n >= webhook.retry.maxAttempts => givenUp(event, n, reason)
        case DeliveryOutcome.Failed(reason, retryAfter)                         =>
          retried.incrementAndGet()
          metric("retried")
          val wait = retryAfter.filter(_ > backoff(n)).getOrElse(backoff(n))
          org.apache.pekko.pattern.after(wait)(attempt(n + 1))(using env.otoroshiActorSystem)
      }
    attempt(1)
  }

  /** an event whose delivery failed for good: to the dead letters when there are some, an alert otherwise */
  private def givenUp(event: CloudEvent, attempts: Int, reason: String): Future[Unit] = {
    failed.incrementAndGet()
    metric("failed")
    error(s"could not deliver event '${event.id}' after $attempts attempt(s): $reason")
    val failure = event.copy(extensions = event.extensions ++ Map(
      "webhookid"       -> JsString(webhook.id),
      "webhookerror"    -> JsString(reason.take(1000)),
      "webhookattempts" -> JsString(attempts.toString)
    ))
    webhook.deadLetter match {
      case None       =>
        EventBrokerWebhookDeliveryFailedAlert(webhook, event, attempts, reason, env).toAnalytics()
        Future.unit
      case Some(dead) =>
        connections.acquire(dead.connection.getOrElse(webhook.connection)) match {
          case Left(why)    =>
            EventBrokerWebhookDeliveryFailedAlert(webhook, event, attempts, s"$reason (and the dead letters are not available: ${why.message})", env).toAnalytics()
            Future.unit
          case Right(lease) =>
            val message = CloudEventBindings.toMessage(failure, lease.client.config.kind, headers = lease.client.capabilities.headers)
            lease
              .releasing(Future.fromTry(Try(lease.client.publish(Seq(message), dead.options))).flatten)
              .map(_.headOption.flatMap(_.error) match {
                case None        =>
                  deadLettered.incrementAndGet()
                  metric("dead_lettered")
                case Some(issue) => EventBrokerWebhookDeliveryFailedAlert(webhook, event, attempts, s"$reason (and the dead letters refused it: $issue)", env).toAnalytics()
              })
              .recover { case NonFatal(e) =>
                EventBrokerWebhookDeliveryFailedAlert(webhook, event, attempts, s"$reason (and the dead letters failed: ${ConnectionManager.describe(e)})", env).toAnalytics()
              }
        }
    }
  }

  /** the events of the connection, delivered: one subscription, until it ends */
  private def subscription(): Future[Source[Unit, ?]] =
    connections.acquire(webhook.connection) match {
      case Left(why)    => Future.failed(new IllegalStateException(s"the connection '${webhook.connection}' is not available: ${why.message}"))
      case Right(lease) =>
        val client = lease.client
        val opts   = options(client.config, client)
        PluginUtils.subscriptionSpec(opts) match {
          case Left(why)  =>
            lease.release()
            Future.failed(new IllegalArgumentException(why))
          case Right(spec) =>
            client.capabilities.refuse(spec, client.config.kind.label) match {
              case Some(why) =>
                lease.release()
                Future.failed(new IllegalArgumentException(why))
              case None      =>
                // the lease goes with the stream, or right away if there is no stream
                Future
                  .fromTry(Try(client.subscribe(spec)))
                  .flatten
                  .map { events =>
                    lease.releasing(
                      events.mapAsync(webhook.maxInFlight) { received =>
                        deliver(CloudEventBindings.fromMessage(received.message, client.config.kind)).flatMap(_ => received.ack())
                      }
                    )
                  }
                  .andThen { case Failure(_) => lease.release() }
            }
        }
    }

  private def startDelivering(): Unit = {
    given Materializer = env.otoroshiMaterializer
    val restarts       = RestartSettings(1.second, 30.seconds, 0.2)
    val source         = RestartSource.withBackoff(restarts) { () =>
      Source.futureSource(subscription().andThen { case Failure(e) => error(ConnectionManager.describe(e)) }).watchTermination() { (_, done) =>
        done.failed.foreach(e => error(s"the subscription failed: ${ConnectionManager.describe(e)}"))
        ()
      }
    }
    val (switch, done) = source.viaMat(KillSwitches.single)(Keep.right).toMat(Sink.ignore)(Keep.both).run()
    running.set(Some(switch))
    logger.info(s"webhook '${webhook.name}' (${webhook.id}) is delivered from this node")
    done.onComplete(_ => running.compareAndSet(Some(switch), None))
  }

  private def stopDelivering(): Unit = running.getAndSet(None).foreach { switch =>
    switch.shutdown()
    logger.info(s"webhook '${webhook.name}' (${webhook.id}) is no longer delivered from this node")
  }

  def stop(): Future[Unit] = {
    stopped = true
    ticker.cancel()
    stopDelivering()
    locks.release(webhook.id).recover { case NonFatal(_) => () }
  }

  def json: Future[JsObject] = locks.owner(webhook.id).recover { case NonFatal(_) => None }.map { owner =>
    Json.obj(
      "id"               -> webhook.id,
      "name"             -> webhook.name,
      "delivering"       -> isDelivering,
      "owner"            -> owner,
      "delivered"        -> delivered.get(),
      "retried"          -> retried.get(),
      "failed"           -> failed.get(),
      "dead_lettered"    -> deadLettered.get(),
      "last_delivery_at" -> lastDelivery.get(),
      "last_error"       -> lastError.get().map((at, message) => Json.obj("at" -> at, "message" -> message))
    )
  }
}

/** the webhooks of this node, fed with the enabled ones by the extension */
final class WebhookManager(env: Env, settings: WebhookManagerSettings, connections: ConnectionManager, locksOf: String => WebhookLocks) {

  /** who this node is, for the locks */
  val node: String = s"${Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("otoroshi")}-${UUID.randomUUID().toString.take(8)}"

  lazy val locks: WebhookLocks = locksOf(node)

  private val runners = new TrieMap[String, WebhookRunner]()
  @volatile private var stopped = false

  private given ExecutionContext = env.otoroshiExecutionContext

  def sync(webhooks: Seq[EventBrokerWebhook]): Unit = if (settings.enabled && !stopped) synchronized {
    val wanted = webhooks.filter(_.enabled).map(w => w.id -> w).toMap
    runners.foreach { (id, runner) =>
      if (!wanted.get(id).exists(_.hash == runner.hash)) {
        runners.remove(id)
        runner.stop()
      }
    }
    wanted.foreach { (id, webhook) =>
      if (!runners.contains(id)) runners.put(id, new WebhookRunner(webhook, env, connections, locks, settings.lockTtl / 3))
    }
  }

  def runner(id: String): Option[WebhookRunner] = runners.get(id)

  def statusJson(id: String): Future[Option[JsObject]] = runners.get(id) match {
    case None         => Future.successful(None)
    case Some(runner) => runner.json.map(Some(_))
  }

  def statusesJson(): Future[Seq[JsObject]] = Future.sequence(runners.values.toSeq.sortBy(_.webhook.id).map(_.json))

  def json: JsObject = Json.obj("node" -> node, "enabled" -> settings.enabled, "locks" -> locks.json)

  def stop(): Future[Unit] = {
    stopped = true
    Future.sequence(runners.values.toSeq.map(_.stop())).map { _ =>
      runners.clear()
      ()
    }
  }
}
