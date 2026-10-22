package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, MqttSettings}
import org.apache.pekko.stream.scaladsl.{Source, SourceQueueWithComplete}
import org.apache.pekko.stream.{Materializer, OverflowStrategy}
import org.apache.pekko.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.*
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, Json}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * One mqtt client per connection. Paho keeps one listener per topic filter: the subscribers of the
 * same filter share one broker subscription, made by the first and removed by the last, and every
 * message is handed to each of them.
 */
class MQTTBrokerClient(val config: EventBrokerConnection, settings: MqttSettings) extends EventBrokerClient with BlockingIo {

  // a broker drops the older of two clients with the same id: every node, every client, needs its own
  val clientId = s"${settings.clientIdPrefix}-${UUID.randomUUID().toString.replace("-", "").take(8)}"
  // in memory: paho's default keeps its in-flight messages in a folder of the working directory
  val client   = new MqttClient(s"tcp://${settings.host}:${settings.port}", clientId, new MemoryPersistence())
  val options  = new MqttConnectOptions()
  options.setAutomaticReconnect(true)
  options.setCleanSession(settings.cleanSession)
  options.setConnectionTimeout(settings.connectionTimeoutSeconds)
  options.setKeepAliveInterval(settings.keepAliveSeconds)
  settings.username.foreach(v => options.setUserName(v))
  settings.password.foreach(v => options.setPassword(v.toCharArray))

  private type Subscriber = SourceQueueWithComplete[ReceivedEvent]

  /** topic filter -> (qos, its subscribers). guarded by itself */
  private val subscribers = mutable.Map.empty[String, (Int, Vector[Subscriber])]

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = false, groups = false, replay = false, keys = false, headers = false)

  private def topicOf(name: Option[String]): String =
    name.orElse(settings.defaultTopic).getOrElse(throw new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    rememberIo(ec)
    client.setCallback(new MqttCallbackExtended {
      // a clean session loses its subscriptions with the connection: subscribe again after a reconnect
      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = if (reconnect) {
        val filters = subscribers.synchronized(subscribers.toSeq.map((filter, entry) => (filter, entry._1)))
        Future(filters.foreach((filter, qos) => Try(client.subscribe(filter, qos, listener(filter)))))(using io)
        ()
      }
      override def connectionLost(cause: Throwable): Unit = ()
      override def messageArrived(topic: String, message: MqttMessage): Unit = ()
      override def deliveryComplete(token: IMqttDeliveryToken): Unit = ()
    })
    client.connect(options)
  }

  // paho reconnects by itself: the state tells whether it is connected right now
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] =
    if (client.isConnected) Future.unit else Future.failed(new IllegalStateException("the mqtt client is not connected"))

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    subscribers.synchronized {
      subscribers.values.foreach(_._2.foreach(_.complete()))
      subscribers.clear()
    }
    Try(if (client.isConnected) client.disconnectForcibly(1000, 1000))
    Try(client.close(true))
    ()
  }

  // paho's publish waits for the broker with qos 1 and 2: on the io pool
  override def publish(events: Seq[EventWrapper], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] = Future {
    events.map { event =>
      try {
        val topic    = topicOf(event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String]))
        val qos      = event.vendorMetadata.get("qos").orElse(opts.select("qos").asOpt[String]).map(_.toInt).getOrElse(settings.defaultQos)
        val retained = event.vendorMetadata.get("retained").orElse(opts.select("retained").asOpt[String]).map(_.toBoolean).getOrElse(settings.retain)
        val message  = new MqttMessage(event.data.toArray)
        message.setQos(qos)
        message.setRetained(retained)
        client.publish(topic, message)
        PublishResult.ok(Json.obj("topic" -> topic, "qos" -> qos, "retained" -> retained))
      } catch {
        case NonFatal(e) => PublishResult.failed(e)
      }
    }
  }(using io)

  private def toEvent(topic: String, message: MqttMessage): ReceivedEvent = ReceivedEvent.unacked(
    EventWrapper(
      key = None,
      data = ByteString(message.getPayload),
      keyContentType = "text",
      dataContentType = "binary",
      headers = Map.empty,
      vendorMetadata = Map(
        "provider"     -> "mqtt",
        "topic"        -> topic,
        "message_id"   -> message.getId.toString,
        "qos"          -> message.getQos.toString,
        "is_retained"  -> message.isRetained.toString,
        "is_duplicate" -> message.isDuplicate.toString
      )
    )
  )

  private def listener(filter: String): IMqttMessageListener = new IMqttMessageListener {
    override def messageArrived(topic: String, message: MqttMessage): Unit = {
      val current = subscribers.synchronized(subscribers.get(filter).map(_._2).getOrElse(Vector.empty))
      current.foreach(_.offer(toEvent(topic, message)))
    }
  }

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = Future {
    val filter = topicOf(spec.topics.headOption)
    val qos    = spec.options.select("qos").asOpt[String].map(_.toInt).getOrElse(settings.defaultQos)
    given Materializer = env.otoroshiMaterializer
    // no flow control on a topic: a subscriber too slow loses the oldest messages
    val (queue, source) = Source.queue[ReceivedEvent](1024, OverflowStrategy.dropHead).preMaterialize()
    // subscribing waits for the broker: done under the lock, so that two first subscribers do not both subscribe
    subscribers.synchronized {
      subscribers.get(filter) match {
        case Some((existing, list)) => subscribers.put(filter, (existing, list :+ queue))
        case None                   =>
          client.subscribe(filter, qos, listener(filter))
          subscribers.put(filter, (qos, Vector(queue)))
      }
    }
    Subscribed(source) { () =>
      Future {
        subscribers.synchronized {
          subscribers.get(filter).foreach { (q, list) =>
            val left = list.filterNot(_ eq queue)
            if (left.isEmpty) {
              subscribers.remove(filter)
              Try(client.unsubscribe(filter))
            } else subscribers.put(filter, (q, left))
          }
        }
      }(using io)
      ()
    }
  }(using io)
}
