package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.ConnectionState
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{Host, RawHeader}
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import otoroshi.api.Otoroshi
import otoroshi.events.OtoroshiEvent
import otoroshi.models.{EntityLocation, RoundRobin}
import otoroshi.next.models.*
import otoroshi.next.plugins.api.NgPluginHelper
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.*
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String
import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest, WSResponse}
import play.core.server.ServerConfig

import java.net.ServerSocket
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/** One server-sent event, as read off the wire. */
final case class SseEvent(lines: Seq[String]) {
  def field(name: String): Seq[String] = lines.collect {
    case line if line.startsWith(s"$name: ") => line.drop(name.length + 2)
    case line if line.startsWith(s"$name:")  => line.drop(name.length + 1)
  }
  def id: Option[String] = field("id").lastOption
  def data: String       = field("data").mkString("\n")
}

/**
 * A real Otoroshi, in this process, with the extension on the classpath.
 *
 * Otoroshi's own functional harness lives in its test sources and is not published, but booting the
 * gateway is a public api of the published jar. One instance is shared by every integration suite: a
 * boot costs about fifteen seconds, worth paying once and not once per suite.
 */
object Gateway {

  private def freePort: Int = {
    val socket = new ServerSocket(0)
    val port   = socket.getLocalPort
    socket.close()
    port
  }

  lazy val port: Int = freePort

  lazy val instance: Otoroshi = {
    // play configures logback itself and goes straight for logback.xml, the one in the otoroshi jar
    if (System.getProperty("logger.resource") == null) System.setProperty("logger.resource", "logback-test.xml")
    val config = ConfigFactory
      .parseString(s"""
        |otoroshi.storage = "inmemory"
        |otoroshi.next.state-sync-interval = 500
        |otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers {
        |  enabled = true
        |  # short enough for the suites to watch a broker go away and come back
        |  connections {
        |    connect-timeout = 5s
        |    retry.min-backoff = 200ms
        |    retry.max-backoff = 2s
        |    healthcheck.interval = 1s
        |    healthcheck.timeout = 3s
        |    healthcheck.max-failures = 2
        |    retire-grace-period = 2s
        |  }
        |}
        |""".stripMargin)
      .resolve()
    val oto    = Otoroshi(
      ServerConfig(address = "0.0.0.0", port = Some(port), rootDir = Files.createTempDirectory("event-brokers-it").toFile),
      config
    )
    oto.startAndStopOnShutdown()
    waitUntilHealthy(oto)
    liftAddressQuota(oto)
    listenToEvents(oto)
    oto
  }

  /** every analytics event otoroshi sent since it started, as json, most recent last */
  val events = new ConcurrentLinkedQueue[JsValue]()

  private def listenToEvents(oto: Otoroshi): Unit = {
    val env      = oto.env
    val listener = env.analyticsActorSystem.actorOf(Props(new Actor {
      def receive: Receive = { case event: OtoroshiEvent => events.add(event.toJson(using env)) }
    }))
    env.analyticsActorSystem.eventStream.subscribe(listener, classOf[OtoroshiEvent])
    ()
  }

  def eventsOf(connectionId: String): Seq[JsValue] =
    events.asScala.toSeq.filter(e => (e \ "connection" \ "id").asOpt[String].contains(connectionId))

  given system: ActorSystem  = instance.system
  given mat: Materializer    = instance.materializer
  given ec: ExecutionContext = instance.executionContext
  def ws: WSClient           = instance.ws

  def extension: EventBrokersExtension =
    instance.env.adminExtensions.extension[EventBrokersExtension].getOrElse(throw new RuntimeException("the extension is not loaded"))

  private def waitUntilHealthy(oto: Otoroshi): Unit = {
    val deadline = System.currentTimeMillis() + 120000L
    var healthy  = false
    while (!healthy && System.currentTimeMillis() < deadline) {
      healthy = Try(
        Await.result(oto.ws.url(s"http://127.0.0.1:$port/health").withRequestTimeout(2.seconds).get(), 5.seconds).status
      ).toOption.contains(200)
      if (!healthy) Thread.sleep(500L)
    }
    if (!healthy) throw new RuntimeException(s"otoroshi did not become healthy on port $port")
    // the router syncs on a tick; a route created before it has run would not be matched yet
    Thread.sleep(2000L)
  }

  /** every call comes from 127.0.0.1: the publishing loops would trip the per address quota */
  private def liftAddressQuota(oto: Otoroshi): Unit = {
    val patch = Json.arr(Json.obj("op" -> "replace", "path" -> "/perIpThrottlingQuota", "value" -> 100000000L))
    val res   = Await.result(
      oto.ws
        .url(s"http://127.0.0.1:$port/api/globalconfig")
        .withHttpHeaders("Host" -> "otoroshi-api.oto.tools", "Content-Type" -> "application/json")
        .withAuth("admin-api-apikey-id", "admin-api-apikey-secret", WSAuthScheme.BASIC)
        .patch(Json.stringify(patch)),
      10.seconds
    )
    if (res.status > 299) throw new RuntimeException(s"could not lift the per address quota: ${res.status} ${res.body}")
  }

  def await[A](f: Future[A], atMost: FiniteDuration = 120.seconds): A = Await.result(f, atMost)

  def uniq(prefix: String): String = s"$prefix-${UUID.randomUUID().toString.take(8)}"

  def eventually(what: String, atMost: FiniteDuration = 30.seconds)(cond: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + atMost.toMillis
    while (!Try(cond).getOrElse(false)) {
      if (System.currentTimeMillis() > deadline) throw new RuntimeException(s"timed out waiting for $what")
      Thread.sleep(100L)
    }
  }

  // -----------------------------------------------------------------------------------------------
  // admin api
  // -----------------------------------------------------------------------------------------------

  def admin(path: String): WSRequest =
    ws.url(s"http://127.0.0.1:$port$path")
      .withHttpHeaders("Host" -> "otoroshi-api.oto.tools", "Content-Type" -> "application/json")
      .withAuth("admin-api-apikey-id", "admin-api-apikey-secret", WSAuthScheme.BASIC)

  val connectionsPath = "/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections"

  private def connectionJson(id: String, kind: String, settings: JsObject): String = Json.stringify(
    Json.obj(
      "id"          -> id,
      "name"        -> s"$kind connection",
      "description" -> "created by the integration tests",
      "tags"        -> Json.arr(),
      "metadata"    -> Json.obj(),
      "kind"        -> kind,
      "settings"    -> settings
    )
  )

  def connectionState(id: String): Option[ConnectionState] = extension.connections.status(id).map(_.state)

  /**
   * creates a connection and waits until the extension state has picked it up, and, unless told
   * otherwise, until this node is connected to its broker
   */
  /** every connection created, in order: a suite deletes its own once done, their clients go with them */
  private val created = new java.util.concurrent.CopyOnWriteArrayList[String]()

  def connectionsCreated: Int = created.size()

  /** deletes the connections created since `mark` (`connectionsCreated` back then) */
  def deleteConnectionsFrom(mark: Int): Unit =
    created.asScala.drop(mark).foreach(id => scala.util.Try(deleteConnection(id)))

  def createConnection(kind: String, settings: JsObject, waitConnected: Boolean = true): String = {
    val id  = s"connection_${UUID.randomUUID()}"
    val res = await(admin(connectionsPath).post(connectionJson(id, kind, settings)))
    if (res.status > 299) throw new RuntimeException(s"could not create the connection: ${res.status} ${res.body}")
    created.add(id)
    eventually(s"connection $id in the extension state")(extension.states.connection(id).isDefined)
    if (waitConnected) eventually(s"connection $id connected", 60.seconds)(connectionState(id).contains(ConnectionState.Connected))
    id
  }

  def updateConnection(id: String, kind: String, settings: JsObject): Unit = {
    val res = await(admin(s"$connectionsPath/$id").put(connectionJson(id, kind, settings)))
    if (res.status > 299) throw new RuntimeException(s"could not update the connection: ${res.status} ${res.body}")
  }

  def deleteConnection(id: String): WSResponse = await(admin(s"$connectionsPath/$id").delete())

  /** what the admin api says of the connections of this node */
  def connectionStatus(id: String): WSResponse =
    await(admin(s"/api/extensions/cloud-apim/event-brokers/connections/$id/_status").get())

  def connectionStatuses(): WSResponse =
    await(admin("/api/extensions/cloud-apim/event-brokers/connections/_status").get())

  def createRoute(plugins: Seq[NgPluginInstance]): NgRoute = {
    val id    = uniq("route")
    val route = NgRoute(
      location = EntityLocation.default,
      id = id,
      name = id,
      description = id,
      enabled = true,
      debugFlow = false,
      capture = false,
      exportReporting = false,
      frontend = NgFrontend(
        domains = Seq(NgDomainAndPath(s"$id.oto.tools")),
        headers = Map.empty,
        cookies = Map.empty,
        query = Map.empty,
        methods = Seq.empty,
        stripPath = true,
        exact = false
      ),
      // never called: every plugin under test answers in place of the backend
      backend = NgBackend(
        targets = Seq(NgTarget(hostname = "127.0.0.1", port = 9, id = "unused.target", tls = false)),
        root = "/",
        rewrite = false,
        loadBalancing = RoundRobin,
        client = NgClientConfig.default
      ),
      plugins = NgPlugins(plugins),
      groups = Seq("default"),
      tags = Seq.empty,
      metadata = Map.empty
    )
    val res   = await(admin("/api/routes").post(Json.stringify(route.json)))
    if (res.status > 299) throw new RuntimeException(s"could not create the route: ${res.status} ${res.body}")
    eventually(s"route $id in the proxy state")(instance.env.proxyState.route(id).isDefined)
    route
  }

  /** what the suites need: the caller may set every option (the policy has a suite of its own) */
  val everythingAllowed: JsObject = Json.obj("allowed_overrides" -> Json.arr("*"))

  /** a route whose only plugin is `P`, bound to the given connection */
  def brokerRoute[P](connectionId: String, config: JsObject = everythingAllowed)(using ClassTag[P]): NgRoute =
    createRoute(
      Seq(
        NgPluginInstance(
          plugin = NgPluginHelper.pluginId[P],
          config = NgPluginInstanceConfig(Json.obj("ref" -> connectionId) ++ config)
        )
      )
    )

  def deleteRoute(route: NgRoute): WSResponse = await(admin(s"/api/routes/${route.id}").delete())

  // -----------------------------------------------------------------------------------------------
  // calling the routes. each call gets its own connection, so ending a stream really hangs up on the
  // gateway, like a client going away would.
  // -----------------------------------------------------------------------------------------------

  def request(
      route: NgRoute,
      method: HttpMethod = HttpMethods.GET,
      path: String = "/",
      headers: Seq[(String, String)] = Seq.empty,
      entity: RequestEntity = HttpEntity.Empty
  ): Future[HttpResponse] = {
    val req = HttpRequest(
      method = method,
      uri = Uri(path),
      headers = (Host(route.frontend.domains.head.domain) +: headers.map((k, v) => RawHeader(k, v))).toList,
      entity = entity
    )
    Source.single(req).via(Http().outgoingConnection("127.0.0.1", port)).runWith(Sink.head)
  }

  def contentType(value: String): ContentType =
    ContentType.parse(value).fold(errors => throw new RuntimeException(s"bad content type $value: $errors"), identity)

  /** posts a body to an ingesting route, returns the status and the json lines it answered */
  def post(
      route: NgRoute,
      path: String,
      body: String,
      ctype: String = "application/json",
      headers: Seq[(String, String)] = Seq.empty
  ): (Int, Seq[JsValue]) = {
    val res  = await(request(route, HttpMethods.POST, path, headers, HttpEntity(contentType(ctype), ByteString(body))))
    val text = await(res.entity.toStrict(30.seconds)).data.utf8String
    (res.status.intValue(), text.split("\n").toSeq.map(_.trim).filter(_.nonEmpty).map(l => Try(Json.parse(l)).getOrElse(JsString(l))))
  }

  /** the body of a streaming response, one element per line */
  def lines(route: NgRoute, path: String, headers: Seq[(String, String)] = Seq.empty): Source[String, ?] =
    Source
      .futureSource(request(route, HttpMethods.GET, path, headers).map { res =>
        res.entity
          .withoutSizeLimit()
          .dataBytes
          .via(Framing.delimiter(ByteString("\n"), 16 * 1024 * 1024, allowTruncation = true))
          .map(_.utf8String)
      })

  def ndjson(route: NgRoute, path: String, headers: Seq[(String, String)] = Seq.empty): Source[JsValue, ?] =
    lines(route, path, headers).filter(_.trim.nonEmpty).map(Json.parse)

  def sse(route: NgRoute, path: String, headers: Seq[(String, String)] = Seq.empty): Source[SseEvent, ?] =
    lines(route, path, headers)
      .statefulMap(() => Vector.empty[String])(
        (acc, line) => if (line.isEmpty) (Vector.empty, Some(acc)) else (acc :+ line, None),
        acc => Some(Some(acc))
      )
      .collect { case Some(acc) if acc.nonEmpty => SseEvent(acc) }

  /** a websocket kept open: what the gateway sends piles up in `received`, `send` writes to it */
  final class WebsocketSession(val received: java.util.concurrent.LinkedBlockingQueue[String], outgoing: org.apache.pekko.stream.BoundedSourceQueue[Message]) {
    def send(text: String): Unit = { outgoing.offer(TextMessage(text)); () }
    def next(timeout: FiniteDuration = 10.seconds): Option[String] = Option(received.poll(timeout.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))
    def close(): Unit = outgoing.complete()
  }

  def websocketSession(route: NgRoute, path: String): WebsocketSession = {
    val received        = new java.util.concurrent.LinkedBlockingQueue[String]()
    val (outgoing, out) = Source.queue[Message](64).preMaterialize()
    val in              = Flow[Message]
      .mapAsync(1) {
        case m: TextMessage   => m.toStrict(10.seconds).map(_.text)
        case m: BinaryMessage => m.toStrict(10.seconds).map(_.data.utf8String)
      }
      .to(Sink.foreach(text => { received.add(text); () }))
    val (upgrade, _)    = Http().singleWebSocketRequest(
      WebSocketRequest(uri = s"ws://127.0.0.1:$port$path", extraHeaders = Seq(Host(route.frontend.domains.head.domain))),
      Flow.fromSinkAndSourceMat(in, out)(Keep.none)
    )
    await(upgrade)
    new WebsocketSession(received, outgoing)
  }

  /**
   * Opens a websocket on the route, sends `out`, keeps the socket open and collects the first `take`
   * text frames the gateway sends back (or whatever came within `within`).
   */
  def websocket(route: NgRoute, path: String, out: Source[Message, ?], take: Int, within: FiniteDuration): Future[Seq[String]] = {
    val in       = Flow[Message]
      .mapAsync(1) {
        case m: TextMessage   => m.toStrict(10.seconds).map(_.text)
        case m: BinaryMessage => m.toStrict(10.seconds).map(_.data.utf8String)
      }
      .take(take.toLong)
      .takeWithin(within)
      .toMat(Sink.seq)(Keep.right)
    val flow     = Flow.fromSinkAndSourceMat(in, out.concat(Source.maybe[Message]))(Keep.left)
    val (_, res) = Http().singleWebSocketRequest(
      WebSocketRequest(uri = s"ws://127.0.0.1:$port$path", extraHeaders = Seq(Host(route.frontend.domains.head.domain))),
      flow
    )
    res
  }
}
