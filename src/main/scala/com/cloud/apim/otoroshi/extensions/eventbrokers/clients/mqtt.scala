package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
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

class MQTTBrokerClient(val config: EventBrokerConnection) extends EventBrokerClient {

  val publisherId = config.settings.select("publisher_id").asOpt[String].getOrElse(UUID.randomUUID().toString)
  val host = config.settings.select("host").asOpt[String].getOrElse("localhost")
  val defaulttopic = config.settings.select("topic").asOpt[String].getOrElse("test")
  val port = config.settings.select("port").asOpt[Int].getOrElse(1883)
  val defaultretained = config.settings.select("retained").asOpt[Boolean].getOrElse(true)
  val defaultqos = config.settings.select("qos").asOpt[Int].getOrElse(0)
  // in memory: paho's default keeps its in-flight messages in a folder of the working directory
  val client = new MqttClient(s"tcp://${host}:${port}", publisherId, new MemoryPersistence())
  val options = new MqttConnectOptions()
  options.setAutomaticReconnect(true)
  options.setCleanSession(true)
  options.setConnectionTimeout(10)
  config.settings.select("username").asOpt[String].filterNot(_.isBlank).foreach(v => options.setUserName(v))
  config.settings.select("password").asOpt[String].filterNot(_.isBlank).foreach(v => options.setPassword(v.toCharArray))

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
    val topic = event.vendorMetadata.get("topic").orElse(opts.select("topic").asOpt[String]).getOrElse(defaulttopic)
    val qos = event.vendorMetadata.get("qos").orElse(opts.select("qos").asOpt[String]).map(_.toInt).getOrElse(defaultqos)
    val retained = event.vendorMetadata.get("retained").orElse(opts.select("retained").asOpt[String]).map(_.toBoolean).getOrElse(defaultretained)
    val msg = new MqttMessage(event.data.toArray)
    msg.setQos(qos)
    msg.setRetained(retained)
    id.foreach(id => msg.setId(id))
    client.publish(topic, msg)
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(defaulttopic)
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