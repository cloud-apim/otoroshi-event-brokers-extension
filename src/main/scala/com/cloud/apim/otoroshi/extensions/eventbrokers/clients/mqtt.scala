package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, MqttSettings}
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.lifecycle.{MqttClientAutoReconnect, MqttClientConnectedContext, MqttClientConnectedListener}
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient
import com.hivemq.client.mqtt.mqtt3.message.auth.Mqtt3SimpleAuth
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3Connect
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe
import com.hivemq.client.mqtt.mqtt3.message.unsubscribe.Mqtt3Unsubscribe
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.mqtt5.datatypes.{Mqtt5UserProperties, Mqtt5UserProperty}
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.Mqtt5Unsubscribe
import com.hivemq.client.mqtt.{MqttClient, MqttClientSslConfig, MqttClientTransportConfig, MqttWebSocketConfig}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, Json}

import java.io.FileInputStream
import java.security.KeyStore
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.jdk.OptionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/** `trust_all`: the client only takes a factory, so a factory of a manager that trusts anything */
private[clients] object TrustAll {
  private val manager = new javax.net.ssl.X509TrustManager {
    override def checkClientTrusted(chain: Array[java.security.cert.X509Certificate], authType: String): Unit = ()
    override def checkServerTrusted(chain: Array[java.security.cert.X509Certificate], authType: String): Unit = ()
    override def getAcceptedIssuers: Array[java.security.cert.X509Certificate] = Array.empty
  }
  val factory: TrustManagerFactory = new TrustManagerFactory(
    new javax.net.ssl.TrustManagerFactorySpi {
      override def engineInit(ks: KeyStore): Unit = ()
      override def engineInit(spec: javax.net.ssl.ManagerFactoryParameters): Unit = ()
      override def engineGetTrustManagers(): Array[javax.net.ssl.TrustManager] = Array(manager)
    },
    new java.security.Provider("cloud-apim-trust-all", "1.0", "trusts every certificate") {},
    "trust-all"
  ) {}
}

/** what the client gets from the broker, whatever the version */
private[clients] final case class MqttIncoming(
    topic: String,
    payload: ByteString,
    qos: Int,
    retained: Boolean,
    contentType: Option[String] = None,
    userProperties: Map[String, String] = Map.empty,
    responseTopic: Option[String] = None,
    messageExpirySeconds: Option[Long] = None
)

/** one version of the protocol */
private[clients] trait MqttDriver {
  def connect(): Future[Unit]
  def disconnect(): Future[Unit]
  def isConnected: Boolean
  def publish(topic: String, message: BrokerMessage, qos: Int, retain: Boolean): Future[Unit]
  def subscribe(filter: String, qos: Int, onMessage: MqttIncoming => Unit): Future[Unit]
  def unsubscribe(filter: String): Future[Unit]
}

private[clients] final class Mqtt5Driver(client: Mqtt5AsyncClient, settings: MqttSettings)(using ec: ExecutionContext) extends MqttDriver {

  override def connect(): Future[Unit] = {
    val connect = Mqtt5Connect
      .builder()
      .cleanStart(settings.cleanSession)
      .sessionExpiryInterval(settings.sessionExpirySeconds)
      .keepAlive(settings.keepAliveSeconds)
      .applyOnWithOpt(settings.username) { (b, user) =>
        b.simpleAuth(Mqtt5SimpleAuth.builder().username(user).applyOnWithOpt(settings.password)((a, p) => a.password(p.getBytes("UTF-8"))).build())
      }
      .build()
    client.connect(connect).asScala.map(_ => ())
  }

  override def disconnect(): Future[Unit] = client.disconnect().asScala.map(_ => ())

  override def isConnected: Boolean = client.getState.isConnected

  // the content type and the headers of the message travel as mqtt 5 properties
  override def publish(topic: String, message: BrokerMessage, qos: Int, retain: Boolean): Future[Unit] = {
    val properties = Mqtt5UserProperties.of(message.headers.toSeq.map((name, value) => Mqtt5UserProperty.of(name, value)).asJava)
    val publish    = Mqtt5Publish
      .builder()
      .topic(topic)
      .qos(MqttQos.fromCode(qos))
      .retain(retain)
      .payload(message.data.toArray)
      .userProperties(properties)
      .applyOnWithOpt(message.metadata.get("content_type"))((b, ct) => b.contentType(ct))
      .applyOnWithOpt(message.metadata.get("response_topic"))((b, t) => b.responseTopic(t))
      .applyOnWithOpt(message.metadata.get("message_expiry_seconds").flatMap(_.toLongOption))((b, s) => b.messageExpiryInterval(s))
      .build()
    client.publish(publish).asScala.flatMap(result => result.getError.toScala.map(Future.failed).getOrElse(Future.unit))
  }

  override def subscribe(filter: String, qos: Int, onMessage: MqttIncoming => Unit): Future[Unit] =
    client
      .subscribe(
        Mqtt5Subscribe.builder().topicFilter(filter).qos(MqttQos.fromCode(qos)).build(),
        (publish: Mqtt5Publish) =>
          onMessage(
            MqttIncoming(
              topic = publish.getTopic.toString,
              payload = ByteString(publish.getPayloadAsBytes),
              qos = publish.getQos.getCode,
              retained = publish.isRetain,
              contentType = publish.getContentType.toScala.map(_.toString),
              userProperties = publish.getUserProperties.asList().asScala.map(p => p.getName.toString -> p.getValue.toString).toMap,
              responseTopic = publish.getResponseTopic.toScala.map(_.toString),
              messageExpirySeconds = publish.getMessageExpiryInterval.toScala
            )
          )
      )
      .asScala
      .flatMap { suback =>
        suback.getReasonCodes.asScala.find(_.isError) match {
          case Some(code) => Future.failed(new IllegalStateException(s"the broker refused the subscription to '$filter': $code"))
          case None       => Future.unit
        }
      }

  override def unsubscribe(filter: String): Future[Unit] =
    client.unsubscribe(Mqtt5Unsubscribe.builder().topicFilter(filter).build()).asScala.map(_ => ())
}

private[clients] final class Mqtt3Driver(client: Mqtt3AsyncClient, settings: MqttSettings)(using ec: ExecutionContext) extends MqttDriver {

  override def connect(): Future[Unit] = {
    val connect = Mqtt3Connect
      .builder()
      .cleanSession(settings.cleanSession)
      .keepAlive(settings.keepAliveSeconds)
      .applyOnWithOpt(settings.username) { (b, user) =>
        b.simpleAuth(Mqtt3SimpleAuth.builder().username(user).applyOnWithOpt(settings.password)((a, p) => a.password(p.getBytes("UTF-8"))).build())
      }
      .build()
    client.connect(connect).asScala.map(_ => ())
  }

  override def disconnect(): Future[Unit] = client.disconnect().asScala.map(_ => ())

  override def isConnected: Boolean = client.getState.isConnected

  // no properties in mqtt 3.1.1: what is not the payload does not travel
  override def publish(topic: String, message: BrokerMessage, qos: Int, retain: Boolean): Future[Unit] =
    client
      .publish(Mqtt3Publish.builder().topic(topic).qos(MqttQos.fromCode(qos)).retain(retain).payload(message.data.toArray).build())
      .asScala
      .map(_ => ())

  override def subscribe(filter: String, qos: Int, onMessage: MqttIncoming => Unit): Future[Unit] =
    client
      .subscribe(
        Mqtt3Subscribe.builder().topicFilter(filter).qos(MqttQos.fromCode(qos)).build(),
        (publish: Mqtt3Publish) =>
          onMessage(MqttIncoming(publish.getTopic.toString, ByteString(publish.getPayloadAsBytes), publish.getQos.getCode, publish.isRetain))
      )
      .asScala
      .flatMap { suback =>
        suback.getReturnCodes.asScala.find(_.isError) match {
          case Some(code) => Future.failed(new IllegalStateException(s"the broker refused the subscription to '$filter': $code"))
          case None       => Future.unit
        }
      }

  override def unsubscribe(filter: String): Future[Unit] =
    client.unsubscribe(Mqtt3Unsubscribe.builder().topicFilter(filter).build()).asScala.map(_ => ())
}

/**
 * MQTT 3.1.1 and 5, with the HiveMQ client, over tcp or websockets, with or without tls.
 *
 * One client per connection. The subscribers of the same topic filter share one broker
 * subscription, made by the first and removed by the last, and every message is handed to each of
 * them. With mqtt 5, a group is a shared subscription (`$share/<group>/<filter>`): its subscribers
 * share the messages. A clean session loses its subscriptions with the connection: they are made
 * again once the client is connected again.
 */
class MQTTBrokerClient(val config: EventBrokerConnection, settings: MqttSettings) extends EventBrokerClient with BlockingIo {

  // a broker drops the older of two clients with the same id: every node, every client, needs its own
  val clientId = s"${settings.clientIdPrefix}-${UUID.randomUUID().toString.replace("-", "").take(8)}"

  private type Subscriber = BoundedSourceQueue[ReceivedEvent]

  /** topic filter -> (qos, its subscribers). guarded by itself */
  private val subscribers = mutable.Map.empty[String, (Int, Vector[Subscriber])]
  private val driverRef   = new AtomicReference[Option[MqttDriver]](None)
  private val connected   = new AtomicBoolean(false)

  private def driver: MqttDriver = driverRef.get().getOrElse(throw new IllegalStateException("the mqtt client is not started"))

  override val capabilities: BrokerCapabilities =
    BrokerCapabilities(acks = false, groups = settings.isV5, replay = false, keys = false, headers = settings.isV5)

  private def topicOf(name: Option[String]): String =
    name.orElse(settings.defaultTopic).getOrElse(throw new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic"))

  /** tls with the certificates of the otoroshi store, or the default trust of the jvm */
  private def sslConfig()(using env: Env): MqttClientSslConfig = {
    val tls     = settings.tls
    val builder = MqttClientSslConfig.builder()
    if (tls.trustAll) {
      builder.trustManagerFactory(TrustAll.factory)
    } else if (tls.clientCerts.nonEmpty || tls.trustedCerts.nonEmpty) {
      val (keystore, truststore, password) = tls.mtlsConfig.toJKS(using env)
      def load(file: java.io.File): KeyStore = {
        val store = KeyStore.getInstance("JKS")
        val in    = new FileInputStream(file)
        try store.load(in, password.toCharArray)
        finally in.close()
        store
      }
      if (tls.trustedCerts.nonEmpty) {
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(load(truststore))
        builder.trustManagerFactory(tmf)
      }
      if (tls.clientCerts.nonEmpty) {
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        kmf.init(load(keystore), password.toCharArray)
        builder.keyManagerFactory(kmf)
      }
    }
    if (!tls.hostnameVerification) builder.hostnameVerifier((_, _) => true)
    builder.build()
  }

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    rememberIo(ec)
    val transport = MqttClientTransportConfig
      .builder()
      .serverHost(settings.host)
      .serverPort(settings.port)
      .socketConnectTimeout(settings.connectionTimeoutSeconds.toLong, TimeUnit.SECONDS)
      .mqttConnectTimeout(settings.connectionTimeoutSeconds.toLong, TimeUnit.SECONDS)
      .applyOnIf(settings.tls.enabled)(_.sslConfig(sslConfig()))
      .applyOnIf(settings.transport == "websocket")(_.webSocketConfig(MqttWebSocketConfig.builder().serverPath(settings.websocketPath).build()))
      .build()
    val reconnect = MqttClientAutoReconnect.builder().initialDelay(1, TimeUnit.SECONDS).maxDelay(30, TimeUnit.SECONDS).build()
    // the first connection is start's; the next ones are reconnections, which need the subscriptions again
    val listener  = new MqttClientConnectedListener {
      override def onConnected(context: MqttClientConnectedContext): Unit =
        if (!connected.compareAndSet(false, true)) resubscribe()
    }
    val built     = MqttClient.builder().identifier(clientId).transportConfig(transport).automaticReconnect(reconnect).addConnectedListener(listener)
    val driver    =
      if (settings.isV5) new Mqtt5Driver(built.useMqttVersion5().buildAsync(), settings)(using ec)
      else new Mqtt3Driver(built.useMqttVersion3().buildAsync(), settings)(using ec)
    driverRef.set(Some(driver))
    driver
  }.flatMap(_.connect())

  private def resubscribe(): Unit = {
    val filters = subscribers.synchronized(subscribers.toSeq.map((filter, entry) => (filter, entry._1)))
    filters.foreach((filter, qos) => driver.subscribe(filter, qos, deliver(filter)))
  }

  // the client reconnects by itself: the state tells whether it is connected right now
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] =
    if (driverRef.get().exists(_.isConnected)) Future.unit else Future.failed(new IllegalStateException("the mqtt client is not connected"))

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = {
    subscribers.synchronized {
      subscribers.values.foreach(_._2.foreach(_.complete()))
      subscribers.clear()
    }
    driverRef.getAndSet(None) match {
      case Some(d) if d.isConnected => d.disconnect().recover { case NonFatal(_) => () }
      case _                        => Future.unit
    }
  }

  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] =
    Future.sequence(events.map { event =>
      try {
        val topic    = topicOf(event.metadata.get("topic").orElse(opts.select("topic").asOpt[String]))
        val qos      = event.metadata.get("qos").orElse(opts.select("qos").asOpt[String]).map(_.toInt).getOrElse(settings.defaultQos)
        val retained = event.metadata.get("retained").orElse(opts.select("retained").asOpt[String]).map(_.toBoolean).getOrElse(settings.retain)
        driver
          .publish(topic, event, qos, retained)
          .map(_ => PublishResult.ok(Json.obj("topic" -> topic, "qos" -> qos, "retained" -> retained)))
          .recover { case NonFatal(e) => PublishResult.failed(e) }
      } catch {
        case NonFatal(e) => Future.successful(PublishResult.failed(e))
      }
    })

  private def toEvent(incoming: MqttIncoming): ReceivedEvent = ReceivedEvent.unacked(
    BrokerMessage(
      data = incoming.payload,
      headers = incoming.userProperties,
      metadata = Map(
        "provider"    -> "mqtt",
        "topic"       -> incoming.topic,
        "qos"         -> incoming.qos.toString,
        "is_retained" -> incoming.retained.toString
      ) ++ incoming.contentType.map("content_type" -> _) ++ incoming.responseTopic.map("response_topic" -> _) ++
        incoming.messageExpirySeconds.map(s => "message_expiry_seconds" -> s.toString)
    )
  )

  private val turns = new java.util.concurrent.atomic.AtomicLong(0L)

  /**
   * every subscriber of a filter gets every message, except for a shared subscription: the broker
   * shares its messages between the nodes, each node shares its own between its subscribers
   */
  private def deliver(filter: String)(incoming: MqttIncoming): Unit = {
    val current = subscribers.synchronized(subscribers.get(filter).map(_._2).getOrElse(Vector.empty))
    if (current.nonEmpty) {
      if (filter.startsWith("$share/")) current((turns.getAndIncrement() % current.size).toInt).offer(toEvent(incoming))
      else current.foreach(_.offer(toEvent(incoming)))
    }
  }

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val topic    = Try(topicOf(spec.topics.headOption))
    val qos      = spec.options.select("qos").asOpt[String].map(_.toInt).getOrElse(settings.defaultQos)
    val badGroup = spec.group.filter(g => g.isEmpty || g.exists(c => c == '/' || c == '+' || c == '#'))
    (topic, badGroup) match {
      case (scala.util.Failure(e), _) => Future.failed(e)
      case (_, Some(g))               => Future.failed(new IllegalArgumentException(s"'$g' cannot be a group: no /, + or #"))
      case (scala.util.Success(t), _) =>
        // a group is a shared subscription: its subscribers share the messages
        val filter = spec.group.map(g => s"$$share/$g/$t").getOrElse(t)
        given Materializer = env.otoroshiMaterializer
        // no flow control on a topic: a subscriber too slow loses the newest messages
        val (queue, source) = Source.queue[ReceivedEvent](1024).preMaterialize()
        val first           = subscribers.synchronized {
          subscribers.get(filter) match {
            case Some((existing, list)) =>
              subscribers.put(filter, (existing, list :+ queue))
              false
            case None                   =>
              subscribers.put(filter, (qos, Vector(queue)))
              true
          }
        }
        def leave(): Unit = {
          val last = subscribers.synchronized {
            subscribers.get(filter).exists { (q, list) =>
              val left = list.filterNot(_ eq queue)
              if (left.isEmpty) subscribers.remove(filter) else subscribers.put(filter, (q, left))
              left.isEmpty
            }
          }
          if (last) driverRef.get().foreach(_.unsubscribe(filter).recover { case NonFatal(_) => () })
        }
        val subscribed = if (first) driver.subscribe(filter, qos, deliver(filter)) else Future.unit
        subscribed
          .map(_ => Subscribed(source)(() => leave()))
          .recoverWith { case NonFatal(e) =>
            leave()
            Future.failed(e)
          }
    }
  }
}
