package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, EventBrokerConnectionKind}
import org.apache.pekko.stream.scaladsl.Source
import otoroshi.env.Env
import otoroshi.models.EntityLocation
import play.api.libs.json.*

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

/** what the fake clients of one connection talk to */
final class FakeBroker {
  @volatile var reachable: Boolean                = true
  /** while set, the clients being started wait for it */
  @volatile var startGate: Option[Promise[Unit]]  = None
  val started                                     = new AtomicInteger(0)
  val clients                                     = new ConcurrentLinkedQueue[FakeClient]()
  def client(version: Int): Option[FakeClient]    = clients.asScala.find(_.version == version)
}

final class FakeClient(val config: EventBrokerConnection, broker: FakeBroker) extends EventBrokerClient {

  @volatile var closed: Boolean = false

  def version: Int     = (config.settings \ "version").asOpt[Int].getOrElse(0)
  def broken: Boolean  = (config.settings \ "broken").asOpt[Boolean].getOrElse(false)

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = {
    broker.started.incrementAndGet()
    broker.clients.add(this)
    broker.startGate.map(_.future).getOrElse(Future.unit).flatMap { _ =>
      if (broker.reachable && !broken) Future.unit else Future.failed(new RuntimeException("connection refused"))
    }
  }

  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] =
    if (broker.reachable && !closed) Future.unit else Future.failed(new RuntimeException("no answer"))

  override def close()(using env: Env, ec: ExecutionContext): Future[Unit] = {
    closed = true
    Future.unit
  }

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = false, groups = false, replay = false, keys = false, headers = false)

  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] =
    Future.successful(events.map(_ => PublishResult.done))

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] =
    Future.successful(Source.empty)
}

/**
 * The lifecycle of the clients, against fake brokers: deterministic, and no docker needed. The manager
 * runs on the env of the in-process otoroshi (its scheduler, its events).
 */
class ConnectionManagerSuite extends munit.FunSuite {

  override val munitTimeout: Duration = 2.minutes

  import Gateway.{eventually, eventsOf}

  val settings = ConnectionManagerSettings(
    connectTimeout = 2.seconds,
    minBackoff = 100.millis,
    maxBackoff = 500.millis,
    healthcheckInterval = 200.millis,
    healthcheckTimeout = 500.millis,
    maxFailedHealthchecks = 5,
    retireGracePeriod = 1.second,
    closeTimeout = 2.seconds
  )

  def connection(id: String, settings: JsObject = Json.obj("version" -> 1)): EventBrokerConnection = EventBrokerConnection(
    location = EntityLocation.default,
    id = id,
    name = id,
    description = "",
    tags = Seq.empty,
    metadata = Map.empty,
    kind = EventBrokerConnectionKind.Redis,
    settings = settings
  )

  def v(n: Int, extra: JsObject = Json.obj()): JsObject = Json.obj("version" -> n) ++ extra

  /** a manager of its own per test, over fake brokers made on demand */
  def withManager(test: (ConnectionManager, String => FakeBroker) => Unit): Unit = {
    val brokers = scala.collection.concurrent.TrieMap.empty[String, FakeBroker]
    val broker  = (id: String) => brokers.getOrElseUpdate(id, new FakeBroker)
    val manager = new ConnectionManager(Gateway.instance.env, settings, c => new FakeClient(c, broker(c.id)))
    manager.start()
    try test(manager, broker)
    finally manager.stop()
  }

  def uniqId(): String = s"connection_${UUID.randomUUID()}"

  def versionOf(lease: ClientLease): Int = lease.client.asInstanceOf[FakeClient].version

  def state(manager: ConnectionManager, id: String): Option[ConnectionState] = manager.status(id).map(_.state)

  /** (previous state, state) of each state change event sent for the connection, in order */
  def transitions(id: String): Seq[(String, String)] = eventsOf(id)
    .filter(e => (e \ "@type").asOpt[String].contains("EventBrokerConnectionEvent"))
    .map(e => ((e \ "previous_state").as[String], (e \ "state").as[String]))

  def alerts(id: String): Seq[JsValue] =
    eventsOf(id).filter(e => (e \ "alert").asOpt[String].contains("EventBrokerConnectionUnavailable"))

  test("connects in the background, and only then hands out its client") {
    withManager { (manager, broker) =>
      val id   = uniqId()
      val gate = Promise[Unit]()
      broker(id).startGate = Some(gate)
      manager.sync(Seq(connection(id)))
      manager.acquire(id) match {
        case Left(Unavailability.NotConnected(_, status)) => assertEquals(status.state, ConnectionState.Connecting)
        case other                                        => fail(s"expected not connected, got $other")
      }
      gate.success(())
      eventually("connected")(manager.acquire(id).isRight)
      eventually("the event")(transitions(id).nonEmpty)
      assertEquals(transitions(id), Seq("connecting" -> "connected"))
    }
  }

  test("an unreachable broker: unavailable, tried again and again, connected once it answers") {
    withManager { (manager, broker) =>
      val id = uniqId()
      broker(id).reachable = false
      manager.sync(Seq(connection(id)))
      eventually("unavailable")(state(manager, id).contains(ConnectionState.Unavailable))
      eventually("a few attempts")(broker(id).started.get() >= 3)
      val status = manager.status(id).get
      assertEquals(status.error, Some("RuntimeException: connection refused"))
      assert(status.nextAttemptAt.isDefined, status.toString)
      manager.acquire(id) match {
        case Left(notConnected: Unavailability.NotConnected) => assert(notConnected.retryAfter >= 1L)
        case other                                           => fail(s"expected not connected, got $other")
      }
      // one alert for the outage, not one per attempt
      eventually("the alert")(alerts(id).nonEmpty)
      assertEquals(alerts(id).size, 1, alerts(id).toString)
      // every failed attempt closed what it opened
      assert(broker(id).clients.asScala.forall(_.closed))
      broker(id).reachable = true
      eventually("connected")(manager.acquire(id).isRight)
    }
  }

  test("a broker that stops answering makes it unavailable until it answers again") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id)))
      eventually("connected")(state(manager, id).contains(ConnectionState.Connected))
      broker(id).reachable = false
      eventually("unavailable")(state(manager, id).contains(ConnectionState.Unavailable))
      assert(manager.acquire(id).isLeft)
      broker(id).reachable = true
      eventually("connected again")(state(manager, id).contains(ConnectionState.Connected))
      // recovered in place, nothing was rebuilt
      assertEquals(broker(id).started.get(), 1)
      eventually("the events")(transitions(id).size == 3)
      assertEquals(transitions(id).map(_._2), Seq("connected", "unavailable", "connected"))
    }
  }

  test("after too many failed healthchecks in a row, the client is rebuilt") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id)))
      eventually("connected")(state(manager, id).contains(ConnectionState.Connected))
      val first = broker(id).client(1).get
      broker(id).reachable = false
      eventually("the first client closed")(first.closed)
      eventually("a new attempt")(broker(id).started.get() >= 2)
      broker(id).reachable = true
      eventually("connected again")(manager.acquire(id).isRight)
      val lease = manager.acquire(id).toOption.get
      assert(lease.client ne first)
      lease.release()
    }
  }

  test("new settings: the previous client serves until the new one is connected, and is closed once released") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id, v(1))))
      eventually("connected")(manager.acquire(id).isRight)
      val held = manager.acquire(id).toOption.get
      val gate = Promise[Unit]()
      broker(id).startGate = Some(gate)
      manager.sync(Seq(connection(id, v(2))))
      eventually("the new client starting")(broker(id).client(2).isDefined)
      // no gap: the previous settings still serve
      val during = manager.acquire(id).toOption.get
      assertEquals(versionOf(during), 1)
      during.release()
      gate.success(())
      eventually("the new settings serving")(manager.acquire(id).toOption.exists(l => { l.release(); versionOf(l) == 2 }))
      // still in use
      assert(!broker(id).client(1).get.closed)
      held.release()
      eventually("the previous client closed")(broker(id).client(1).get.closed)
      assert(!broker(id).client(2).get.closed)
    }
  }

  test("a use never released does not keep the previous client open past the grace period") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id, v(1))))
      eventually("connected")(manager.acquire(id).isRight)
      manager.acquire(id) // never released
      manager.sync(Seq(connection(id, v(2))))
      eventually("the new settings serving")(manager.acquire(id).toOption.exists(l => { l.release(); versionOf(l) == 2 }))
      assert(!broker(id).client(1).get.closed)
      eventually("the previous client closed", 5.seconds)(broker(id).client(1).get.closed)
    }
  }

  test("new settings that never connect leave the previous ones serving, and say why") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id, v(1))))
      eventually("connected")(manager.acquire(id).isRight)
      manager.sync(Seq(connection(id, v(2, Json.obj("broken" -> true)))))
      eventually("the new settings failing") {
        manager.statusJson(id).exists(s => (s \ "pending" \ "state").asOpt[String].contains("unavailable"))
      }
      val lease = manager.acquire(id).toOption.get
      assertEquals(versionOf(lease), 1)
      lease.release()
      assertEquals((manager.statusJson(id).get \ "state").as[String], "connected")
      assertEquals((manager.statusJson(id).get \ "pending" \ "error").asOpt[String], Some("RuntimeException: connection refused"))
      // fixed settings replace both
      manager.sync(Seq(connection(id, v(3))))
      eventually("the fixed settings serving")(manager.acquire(id).toOption.exists(l => { l.release(); versionOf(l) == 3 }))
      eventually("the broken one given up")(broker(id).clients.asScala.filter(_.version == 2).forall(_.closed))
    }
  }

  test("a removed connection hands out nothing and closes its client") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id)))
      eventually("connected")(manager.acquire(id).isRight)
      manager.sync(Seq.empty)
      assertEquals(manager.acquire(id), Left(Unavailability.UnknownConnection(id)))
      eventually("closed")(broker(id).client(1).get.closed)
      assertEquals(manager.statusJson(id), None)
    }
  }

  test("an unchanged connection is left alone") {
    withManager { (manager, broker) =>
      val id = uniqId()
      manager.sync(Seq(connection(id)))
      eventually("connected")(manager.acquire(id).isRight)
      (0 until 5).foreach(_ => manager.sync(Seq(connection(id))))
      Thread.sleep(300L)
      assertEquals(broker(id).started.get(), 1)
    }
  }

  test("stopping closes every client") {
    val brokers = scala.collection.concurrent.TrieMap.empty[String, FakeBroker]
    val manager = new ConnectionManager(Gateway.instance.env, settings, c => new FakeClient(c, brokers.getOrElseUpdate(c.id, new FakeBroker)))
    manager.start()
    val ids     = Seq(uniqId(), uniqId())
    manager.sync(ids.map(id => connection(id)))
    ids.foreach(id => eventually(s"$id connected")(manager.acquire(id).isRight))
    manager.stop()
    assert(brokers.values.flatMap(_.clients.asScala).forall(_.closed))
  }
}
