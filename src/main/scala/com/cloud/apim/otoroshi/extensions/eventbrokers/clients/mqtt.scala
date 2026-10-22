package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, MqttSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttMessageListener, MqttClient, MqttConnectOptions, MqttMessage}
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.{JsObject, JsValue}
import reactor.core.publisher.Sinks

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MQTTBrokerClient(val config: EventBrokerConnection, settings: MqttSettings) extends EventBrokerClient {

  // a broker drops the older of two clients with the same id: every node, every client, needs its own
  val clientId = s"${settings.clientIdPrefix}-${UUID.randomUUID().toString.replace("-", "").take(8)}"
  // in memory: paho's default keeps its in-flight messages in a folder of the working directory
  val client = new MqttClient(s"tcp://${settings.host}:${settings.port}", clientId, new MemoryPersistence())
  val options = new MqttConnectOptions()
  options.setAutomaticReconnect(true)
  options.setCleanSession(settings.cleanSession)
  options.setConnectionTimeout(settings.connectionTimeoutSeconds)
  options.setKeepAliveInterval(settings.keepAliveSeconds)
  settings.username.foreach(v => options.setUserName(v))
  settings.password.foreach(v => options.setPassword(v.toCharArray))

  private def topicOf(name: Option[String]): String = name.orElse(settings.defaultTopic)
    .getOrElse(throw new IllegalArgumentException("no topic: none in the call, and the connection has no default_topic"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future(client.connect(options))

  // paho reconnects by itself: the state tells whether it is connected right now
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] =
    if (client.isConnected) Future.unit else Future.failed(new IllegalStateException("the mqtt client is not connected"))

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    Try(if (client.isConnected) client.disconnectForcibly(1000, 1000))
    Try(client.close(true))
    ()
  }

  // TODO: add options to publish enriched event
  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(using env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val id = event.vendorMetadata.get("id").orElse(opts.select("id").asOpt[String]).map(_.toInt)
    val topic = topicOf(event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String]))
    val qos = event.vendorMetadata.get("qos").orElse(opts.select("qos").asOpt[String]).map(_.toInt).getOrElse(settings.defaultQos)
    val retained = event.vendorMetadata.get("retained").orElse(opts.select("retained").asOpt[String]).map(_.toBoolean).getOrElse(settings.retain)
    val msg = new MqttMessage(event.data.toArray)
    msg.setQos(qos)
    msg.setRetained(retained)
    id.foreach(id => msg.setId(id))
    client.publish(topic, msg)
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {
    val topic = topicOf(opts.select("topic").asOpt[String])
    val hotSource = Sinks.many().unicast().onBackpressureBuffer[EventWrapper]()
    val hotFlux   = hotSource.asFlux()
    client.subscribe(topic, new IMqttMessageListener {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        hotSource.tryEmitNext(
          EventWrapper(
            key = Option(message.getId).map(_.toString.byteString),
            data = ByteString(message.getPayload()),
            keyContentType = "text",
            dataContentType = "binary",
            headers = Map.empty,
            vendorMetadata = Map(
              "provider" -> "mqtt",
              "topic" -> topic,
              "qos" -> message.getQos.toString,
              "is_retained" -> message.isRetained.toString,
              "is_duplicate" -> message.isDuplicate.toString,
            )
          )
        )
      }
    })
    // TODO: handle error and flux end
    Source.fromPublisher(hotFlux)
  }
}