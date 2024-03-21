package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import org.eclipse.paho.client.mqttv3.{IMqttMessageListener, MqttClient, MqttConnectOptions, MqttMessage}
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue}
import reactor.core.publisher.Sinks

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MQTTBrokerClient(val config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  val publisherId = config.settings.select("publisher_id").asOpt[String].getOrElse(UUID.randomUUID().toString)
  val host = config.settings.select("host").asOpt[String].getOrElse("localhost")
  val defaulttopic = config.settings.select("topic").asOpt[String].getOrElse("test")
  val port = config.settings.select("port").asOpt[Int].getOrElse(1883)
  val defaultretained = config.settings.select("retained").asOpt[Boolean].getOrElse(true)
  val defaultqos = config.settings.select("qos").asOpt[Int].getOrElse(0)
  val client = new MqttClient(s"tcp://${host}:${port}", publisherId)
  val options = new MqttConnectOptions()
  options.setAutomaticReconnect(true)
  options.setCleanSession(true)
  options.setConnectionTimeout(10)
  config.settings.select("username").asOpt[String].filterNot(_.isBlank).foreach(v => options.setUserName(v))
  config.settings.select("password").asOpt[String].filterNot(_.isBlank).foreach(v => options.setPassword(v.toCharArray))
  client.connect(options)

  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    client.close()
    ().vfuture
  }

  override def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val id = opts.select("id").asOpt[String].map(_.toInt)
    val topic = opts.select("topic").asOpt[String].getOrElse(defaulttopic)
    val qos = opts.select("qos").asOpt[String].map(_.toInt).getOrElse(defaultqos)
    val retained = opts.select("retained").asOpt[String].map(_.toBoolean).getOrElse(defaultretained)
    val msg = new MqttMessage(event.toArray)
    msg.setQos(qos)
    msg.setRetained(retained)
    id.foreach(id => msg.setId(id))
    client.publish(topic, msg)
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val topic = opts.select("topic").asOpt[String].getOrElse(defaulttopic)
    val hotSource = Sinks.many().unicast().onBackpressureBuffer[EventWrapper]()
    val hotFlux   = hotSource.asFlux()
    client.subscribe(topic, new IMqttMessageListener {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        hotSource.tryEmitNext(EventWrapper(None, ByteString(message.getPayload)))
      }
    })
    // TODO: handle error and flux end
    Source.fromPublisher(hotFlux)
  }
}