package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.rabbitmq.client.{CancelCallback, ConnectionFactory, ConsumerShutdownSignalCallback, DeliverCallback, Delivery, ShutdownSignalException}
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.JsValue
import reactor.core.publisher.Sinks

import scala.concurrent.{ExecutionContext, Future}

class RabbitMqClient(config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new RabbitMqClient")

  val queueName = "testing"
  val factory = new ConnectionFactory()
  factory.setHost("localhost")
  factory.setPort(5672)
  factory.setVirtualHost("/")
  val connection = factory.newConnection()
  val channel = connection.createChannel()
  channel.queueDeclare(queueName, false, false, false, null);

  override def publish(event: ByteString)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    // println(s"publish to '${queueName}' - ${event.utf8String}")
    channel.basicPublish("", queueName, null, event.toArray)
    Right(None).vfuture
  }

  override def consume()(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val hotSource = Sinks.many().unicast().onBackpressureBuffer[EventWrapper]()
    val hotFlux   = hotSource.asFlux()
    val cb = new DeliverCallback() {
      override def handle(consumerTag: String, message: Delivery): Unit = {
        val data = ByteString(message.getBody)
        // println(s"received to '${queueName}' - ${data.utf8String}")
        hotSource.tryEmitNext(EventWrapper(None, data))
      }
    }
    val cancelCb = new CancelCallback() {
      override def handle(consumerTag: String): Unit = {
        hotSource.tryEmitComplete()
      }
    }
    val shutdownCb = new ConsumerShutdownSignalCallback() {
      override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {
        hotSource.tryEmitComplete()
      }
    }
    channel.basicConsume(queueName, true, cb, cancelCb, shutdownCb)
    Source.fromPublisher(hotFlux)
  }
}
