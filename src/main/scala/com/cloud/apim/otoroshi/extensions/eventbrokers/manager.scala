package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import org.apache.pekko.actor.{Cancellable, Scheduler}
import org.apache.pekko.stream.scaladsl.Source
import otoroshi.env.Env
import play.api.libs.json.*
import play.api.{Configuration, Logger}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, RejectedExecutionException, ThreadFactory, TimeUnit, TimeoutException}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

final case class ConnectionManagerSettings(
    connectTimeout: FiniteDuration,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    healthcheckInterval: FiniteDuration,
    healthcheckTimeout: FiniteDuration,
    maxFailedHealthchecks: Int,
    retireGracePeriod: FiniteDuration,
    closeTimeout: FiniteDuration
) {

  /** the delay before attempt `failures + 1`: doubles from `minBackoff` up to `maxBackoff`, plus up to 20% of jitter */
  def backoff(failures: Int): FiniteDuration = {
    val base = math.min(maxBackoff.toMillis.toDouble, minBackoff.toMillis.toDouble * math.pow(2, math.max(0, failures - 1).toDouble))
    (base * (1.0 + Random.nextDouble() * 0.2)).toLong.millis
  }
}

object ConnectionManagerSettings {

  val default: ConnectionManagerSettings = ConnectionManagerSettings(
    connectTimeout = 10.seconds,
    minBackoff = 1.second,
    maxBackoff = 30.seconds,
    healthcheckInterval = 10.seconds,
    healthcheckTimeout = 5.seconds,
    maxFailedHealthchecks = 3,
    retireGracePeriod = 30.seconds,
    closeTimeout = 10.seconds
  )

  /** reads the `connections` block of the extension configuration */
  def from(configuration: Configuration): ConnectionManagerSettings = {
    def duration(key: String, default: FiniteDuration): FiniteDuration =
      configuration.getOptional[FiniteDuration](s"connections.$key").getOrElse(default)
    ConnectionManagerSettings(
      connectTimeout = duration("connect-timeout", default.connectTimeout),
      minBackoff = duration("retry.min-backoff", default.minBackoff),
      maxBackoff = duration("retry.max-backoff", default.maxBackoff),
      healthcheckInterval = duration("healthcheck.interval", default.healthcheckInterval),
      healthcheckTimeout = duration("healthcheck.timeout", default.healthcheckTimeout),
      maxFailedHealthchecks = configuration.getOptional[Int]("connections.healthcheck.max-failures").getOrElse(default.maxFailedHealthchecks),
      retireGracePeriod = duration("retire-grace-period", default.retireGracePeriod),
      closeTimeout = duration("close-timeout", default.closeTimeout)
    )
  }
}

enum ConnectionState(val name: String) {
  /** the first connection attempt is running */
  case Connecting  extends ConnectionState("connecting")
  /** connected, and the last healthcheck went fine */
  case Connected   extends ConnectionState("connected")
  /** could not connect (and will try again), or lost the broker */
  case Unavailable extends ConnectionState("unavailable")
  /** replaced by newer settings, or deleted */
  case Closed      extends ConnectionState("closed")
}

final case class ConnectionStatus(
    state: ConnectionState,
    since: Long,
    error: Option[String] = None,
    /** failed attempts in a row when it could not connect, failed healthchecks in a row once connected */
    failures: Int = 0,
    lastCheckAt: Option[Long] = None,
    nextAttemptAt: Option[Long] = None
) {
  def json: JsObject = Json.obj(
    "state"           -> state.name,
    "since"           -> since,
    "error"           -> error,
    "failures"        -> failures,
    "last_check_at"   -> lastCheckAt,
    "next_attempt_at" -> nextAttemptAt
  )
}

/** why no client could be handed out */
sealed trait Unavailability {
  def connectionId: String
  def message: String
}

object Unavailability {
  final case class UnknownConnection(connectionId: String) extends Unavailability {
    def message: String = s"there is no event broker connection '$connectionId'"
  }
  final case class NotConnected(connectionId: String, status: ConnectionStatus) extends Unavailability {
    def message: String = s"the event broker connection '$connectionId' is ${status.state.name}${status.error.map(e => s": $e").getOrElse("")}"

    /** seconds until it is worth trying again */
    def retryAfter: Long = status.nextAttemptAt
      .map(at => math.max(1L, (at - System.currentTimeMillis()) / 1000L + 1L))
      .getOrElse(5L)
  }
}

/** a client handed out for one use (a publish, a stream). `release` it once done; releasing twice is harmless */
final class ClientLease(val client: EventBrokerClient, onRelease: () => Unit) {

  private val released = new AtomicBoolean(false)

  def release(): Unit = if (released.compareAndSet(false, true)) onRelease()

  /** releases the lease when the source completes, fails or is cancelled */
  def releasing[A, M](source: Source[A, M]): Source[A, M] =
    source.watchTermination() { (mat, done) =>
      done.onComplete(_ => release())(using ExecutionContext.parasitic)
      mat
    }

  def releasing[A](future: Future[A]): Future[A] = {
    future.onComplete(_ => release())(using ExecutionContext.parasitic)
    future
  }
}

/**
 * One version (one settings hash) of one connection, and the client built from it.
 *
 * It connects in the background, again and again with a growing delay until it works, then checks the
 * broker periodically: a broker that stops answering makes it unavailable, and after a few failed checks
 * in a row the client is closed and built again. It only hands out its client while connected.
 *
 * Once retired, it hands out nothing more, and its client is closed as soon as the uses already handed
 * out are released, or after the grace period at the latest.
 */
final class ConnectionHandle private[eventbrokers] (
    val connection: EventBrokerConnection,
    manager: ConnectionManager,
    onConnected: ConnectionHandle => Unit
) {

  import manager.given

  private val statusRef      = new AtomicReference(ConnectionStatus(ConnectionState.Connecting, System.currentTimeMillis()))
  private val clientRef      = new AtomicReference[Option[EventBrokerClient]](None)
  private val usages         = new AtomicInteger(0)
  private val retired        = new AtomicBoolean(false)
  private val closed         = new AtomicBoolean(false)
  private val checking       = new AtomicBoolean(false)
  private val pendingAttempt = new AtomicReference[Option[Cancellable]](None)

  def hash: String               = connection.hash
  def status: ConnectionStatus   = statusRef.get()
  def isConnected: Boolean       = status.state == ConnectionState.Connected
  def isRetired: Boolean         = retired.get()
  def activeUsages: Int          = usages.get()

  def start(): Unit = attempt(1)

  private def attempt(n: Int): Unit = if (!retired.get()) {
    Try(manager.factory(connection)) match {
      case Failure(e)      => failed(n, e)
      case Success(client) =>
        val starting = Future(client.start()(using manager.env, manager.blockingEc))(using manager.blockingEc).flatten
        manager
          .bounded(starting, manager.settings.connectTimeout, "connecting")
          .onComplete {
            case Success(_) if !retired.get() =>
              clientRef.set(Some(client))
              update(_.copy(state = ConnectionState.Connected, error = None, failures = 0, nextAttemptAt = None))
              onConnected(this)
            case Success(_)                   => manager.closeQuietly(client)
            case Failure(e)                   =>
              // a start that timed out may still be opening things: close once it is done
              starting.onComplete(_ => manager.closeQuietly(client))
              failed(n, e)
          }
    }
  }

  private def failed(n: Int, e: Throwable): Unit = if (!retired.get()) {
    val delay = manager.settings.backoff(n)
    update(
      _.copy(
        state = ConnectionState.Unavailable,
        error = Some(ConnectionManager.describe(e)),
        failures = n,
        nextAttemptAt = Some(System.currentTimeMillis() + delay.toMillis)
      )
    )
    pendingAttempt.set(Some(manager.scheduler.scheduleOnce(delay)(attempt(n + 1))))
  }

  /** one healthcheck, unless one is already running. called periodically by the manager */
  def check(): Unit = clientRef.get() match {
    case Some(client) if !retired.get() && checking.compareAndSet(false, true) =>
      manager
        .bounded(Future(client.healthcheck()(using manager.env, manager.blockingEc))(using manager.blockingEc).flatten, manager.settings.healthcheckTimeout, "healthcheck")
        .onComplete { result =>
          checking.set(false)
          val now = System.currentTimeMillis()
          result match {
            case _ if retired.get()        => ()
            case Success(_)                =>
              update(_.copy(state = ConnectionState.Connected, error = None, failures = 0, lastCheckAt = Some(now), nextAttemptAt = None))
            case Failure(e)                =>
              val failures = if (status.state == ConnectionState.Connected) 1 else status.failures + 1
              update(_.copy(state = ConnectionState.Unavailable, error = Some(ConnectionManager.describe(e)), failures = failures, lastCheckAt = Some(now)))
              if (failures >= manager.settings.maxFailedHealthchecks) rebuild(client)
          }
        }
    case _ => ()
  }

  /** the client does not recover by itself: drop it and connect again from scratch */
  // compareAndSet compares references: it needs the very Option instance that is in there
  private def rebuild(client: EventBrokerClient): Unit = {
    val current = clientRef.get()
    if (current.exists(_ eq client) && clientRef.compareAndSet(current, None)) rebuildNow(client)
  }

  private def rebuildNow(client: EventBrokerClient): Unit = {
    manager.logger.warn(s"rebuilding the client of event broker connection '${connection.id}' after ${status.failures} failed healthchecks")
    manager.closeQuietly(client)
    attempt(1)
  }

  def acquire(): Option[ClientLease] =
    if (retired.get() || !isConnected) None
    else
      clientRef.get().map { client =>
        usages.incrementAndGet()
        new ClientLease(client, () => release())
      }

  private def release(): Unit = if (usages.decrementAndGet() <= 0 && retired.get()) closeNow()

  def retire(grace: FiniteDuration): Unit = if (retired.compareAndSet(false, true)) {
    pendingAttempt.getAndSet(None).foreach(_.cancel())
    update(_.copy(state = ConnectionState.Closed, nextAttemptAt = None))
    if (usages.get() <= 0 || grace <= Duration.Zero) closeNow()
    else manager.scheduler.scheduleOnce(grace)(closeNow())
  }

  private def closeNow(): Unit = if (closed.compareAndSet(false, true)) clientRef.getAndSet(None).foreach(manager.closeQuietly)

  private def update(f: ConnectionStatus => ConnectionStatus): Unit = {
    val (previous, next) = synchronized {
      val previous = statusRef.get()
      val changed  = f(previous)
      val next     = if (changed.state != previous.state) changed.copy(since = System.currentTimeMillis()) else changed
      statusRef.set(next)
      (previous, next)
    }
    if (previous.state != next.state) manager.stateChanged(connection, previous, next)
  }
}

/**
 * Every version of one connection. The active handle serves the requests. New settings get a handle of
 * their own, which takes over once it is connected: requests keep being served by the previous settings
 * in the meantime, and keep being so if the new ones never connect (the status says why).
 */
final class ConnectionSlot private[eventbrokers] (val id: String, manager: ConnectionManager) {

  @volatile private var active: Option[ConnectionHandle] = None
  @volatile private var next: Option[ConnectionHandle]   = None

  /** the hash of the latest settings offered */
  def hash: Option[String] = next.orElse(active).map(_.hash)

  def handles: Seq[ConnectionHandle] = active.toSeq ++ next.toSeq

  def offer(connection: EventBrokerConnection): Unit = synchronized {
    next.foreach(_.retire(Duration.Zero))
    next = None
    val handle = new ConnectionHandle(connection, manager, promote)
    // nothing worth keeping: switch at once
    if (active.forall(!_.isConnected)) {
      active.foreach(_.retire(Duration.Zero))
      active = Some(handle)
    } else {
      next = Some(handle)
    }
    handle.start()
  }

  private def promote(handle: ConnectionHandle): Unit = synchronized {
    if (next.contains(handle)) {
      val previous = active
      active = next
      next = None
      previous.foreach(_.retire(manager.settings.retireGracePeriod))
    }
  }

  def acquire(): Either[Unavailability, ClientLease] = active match {
    case None         => Left(Unavailability.UnknownConnection(id))
    case Some(handle) => handle.acquire().toRight(Unavailability.NotConnected(id, handle.status))
  }

  def status: Option[ConnectionStatus] = active.map(_.status)

  def check(): Unit = handles.foreach(_.check())

  def remove(grace: FiniteDuration): Unit = synchronized {
    next.foreach(_.retire(Duration.Zero))
    active.foreach(_.retire(grace))
    next = None
  }

  def json: JsObject = {
    val connection = next.orElse(active).map(_.connection)
    Json.obj(
      "id"   -> id,
      "name" -> connection.map(_.name),
      "kind" -> connection.map(_.kind.name)
    ) ++ active.map(_.status.json).getOrElse(Json.obj()) ++ Json.obj(
      // newer settings, not connected yet: the previous ones still serve the requests
      "pending" -> next.map(_.status.json)
    )
  }
}

/**
 * Owns the broker clients of this node: one per connection, built in the background from the extension
 * state, checked periodically, replaced when the settings change and closed when the connection goes.
 * Plugins never build a client: they `acquire` the one of a connection, or get told why it is not there.
 */
final class ConnectionManager(
    val env: Env,
    val settings: ConnectionManagerSettings,
    val factory: EventBrokerConnection => EventBrokerClient
) {

  val logger: Logger = Logger("cloud-apim-event-brokers-connections")

  given ec: ExecutionContext = env.otoroshiExecutionContext

  def scheduler: Scheduler = env.otoroshiScheduler

  // connecting, checking and closing block in most broker clients: never on otoroshi's own threads
  private val pool                         = Executors.newCachedThreadPool(new ThreadFactory {
    private val counter = new AtomicLong(0L)
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, s"cloud-apim-event-brokers-io-${counter.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  })
  val blockingEc: ExecutionContext         = new ExecutionContext {
    private val underlying = ExecutionContext.fromExecutorService(pool)
    // once stopped, what is still to close (a start that was running) closes elsewhere
    override def execute(runnable: Runnable): Unit =
      try underlying.execute(runnable)
      catch { case _: RejectedExecutionException => ExecutionContext.global.execute(() => blocking(runnable.run())) }
    override def reportFailure(cause: Throwable): Unit = logger.error("error in the event brokers io pool", cause)
  }
  private val slots                        = new TrieMap[String, ConnectionSlot]()
  private val ticker                       = new AtomicReference[Option[Cancellable]](None)
  private val stopped                      = new AtomicBoolean(false)

  def start(): Unit = {
    val interval = settings.healthcheckInterval
    ticker.getAndSet(Some(scheduler.scheduleAtFixedRate(interval, interval)(() => tick()))).foreach(_.cancel())
  }

  /** follows the connections of the extension state: called at every state sync */
  def sync(connections: Seq[EventBrokerConnection]): Unit = if (!stopped.get()) synchronized {
    connections.foreach { connection =>
      slots.get(connection.id) match {
        case Some(slot) if slot.hash.contains(connection.hash) => ()
        case Some(slot)                                        => slot.offer(connection)
        case None                                              =>
          val slot = new ConnectionSlot(connection.id, this)
          slots.put(connection.id, slot)
          slot.offer(connection)
      }
    }
    val ids = connections.map(_.id).toSet
    slots.keySet.diff(ids).foreach(id => slots.remove(id).foreach(_.remove(settings.retireGracePeriod)))
  }

  def acquire(id: String): Either[Unavailability, ClientLease] =
    slots.get(id).toRight(Unavailability.UnknownConnection(id)).flatMap(_.acquire())

  def status(id: String): Option[ConnectionStatus] = slots.get(id).flatMap(_.status)

  def statusJson(id: String): Option[JsObject] = slots.get(id).map(_.json)

  def statusesJson(): Seq[JsObject] = slots.values.toSeq.sortBy(_.id).map(_.json)

  /** every handle, retired ones excepted: for the tests */
  def handles(id: String): Seq[ConnectionHandle] = slots.get(id).toSeq.flatMap(_.handles)

  private def tick(): Unit = try {
    slots.values.foreach(_.check())
    if (env.metricsEnabled) {
      val states = slots.values.flatMap(_.status).groupBy(_.state).view.mapValues(_.size.toLong).toMap
      ConnectionState.values.filterNot(_ == ConnectionState.Closed).foreach { state =>
        env.metrics.markLong(s"cloud-apim.event-brokers.connections.${state.name}", states.getOrElse(state, 0L))
      }
    }
  } catch {
    case NonFatal(e) => logger.error("error while checking the event broker connections", e)
  }

  def stop(): Unit = if (stopped.compareAndSet(false, true)) {
    ticker.getAndSet(None).foreach(_.cancel())
    slots.values.foreach(_.remove(Duration.Zero))
    slots.clear()
    // lets the clients close, then lets go of the threads
    pool.shutdown()
    if (!pool.awaitTermination(settings.closeTimeout.toMillis, TimeUnit.MILLISECONDS)) pool.shutdownNow()
    ()
  }

  /** `f`, or a TimeoutException after `timeout`. what `f` is doing goes on: only the wait is bounded */
  def bounded[A](f: Future[A], timeout: FiniteDuration, what: String): Future[A] =
    Future.firstCompletedOf(
      Seq(f, org.apache.pekko.pattern.after(timeout, scheduler)(Future.failed(new TimeoutException(s"$what timed out after $timeout"))))
    )

  def closeQuietly(client: EventBrokerClient): Unit =
    bounded(Future(client.close()(using env, blockingEc))(using blockingEc).flatten, settings.closeTimeout, "closing")
      .recover { case NonFatal(e) =>
        logger.warn(s"error while closing a client of event broker connection '${client.config.id}': ${ConnectionManager.describe(e)}")
      }
      .onComplete(_ => ())

  private[eventbrokers] def stateChanged(connection: EventBrokerConnection, previous: ConnectionStatus, next: ConnectionStatus): Unit = try {
    next.state match {
      case ConnectionState.Connected   => logger.info(s"event broker connection '${connection.id}' (${connection.kind.name}) is connected")
      case ConnectionState.Unavailable =>
        logger.warn(s"event broker connection '${connection.id}' (${connection.kind.name}) is unavailable: ${next.error.getOrElse("unknown error")}")
      case _                           => ()
    }
    given Env = env
    EventBrokerConnectionEvent(connection, previous, next, env).toAnalytics()
    if (next.state == ConnectionState.Unavailable) EventBrokerConnectionUnavailableAlert(connection, previous, next, env).toAnalytics()
    if (env.metricsEnabled) env.metrics.counterInc(s"cloud-apim.event-brokers.connections.transitions.${next.state.name}")
  } catch {
    case NonFatal(e) => logger.error("error while reporting a state change of an event broker connection", e)
  }
}

object ConnectionManager {

  /** a readable cause: the innermost message, which is where broker clients put the useful part */
  def describe(e: Throwable): String = {
    val messages = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).toSeq
    messages.reverse
      .collectFirst { case t if Option(t.getMessage).exists(_.trim.nonEmpty) => s"${t.getClass.getSimpleName}: ${t.getMessage}" }
      .getOrElse(e.getClass.getSimpleName)
  }
}
