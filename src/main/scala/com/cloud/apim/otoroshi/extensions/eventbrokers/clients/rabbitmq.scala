package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.rabbitmq.client._
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue}
import reactor.core.publisher.Sinks

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class RabbitMqBrokerClient(config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new RabbitMqClient")

  val factory = new ConnectionFactory()
  factory.setHost(config.settings.select("host").asOpt[String].getOrElse("localhost"))
  factory.setPort(config.settings.select("port").asOpt[Int].getOrElse(5672))
  factory.setVirtualHost(config.settings.select("virtual_host").asOpt[String].getOrElse("/"))
  config.settings.select("username").asOpt[String].foreach(v => factory.setUsername(v))
  config.settings.select("password").asOpt[String].foreach(v => factory.setPassword(v))
  val connection = factory.newConnection()
  val channel = connection.createChannel()

  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    channel.close()
    connection.close()
    ().vfuture
  }

  override def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    // println(s"publish to '${queueName}' - ${event.utf8String}")
    val queueName = opts.select("queue").asOpt[String]
      .orElse(config.settings.select("queue").asOpt[String])
      .getOrElse("testing")
    val routingKey = opts.select("routing_key").asOpt[String]
      .orElse(config.settings.select("routing_key").asOpt[String])
    if (env.isDev) {
      channel.queueDeclare(queueName, false, false, false, null)
    }
    routingKey match {
      case Some(routingKey) => channel.basicPublish(queueName, routingKey, null, event.toArray)
      case None => channel.basicPublish("", queueName, null, event.toArray)
    }
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
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
    // TODO: handle cancel, shutdown, end of stream, etc
    val queueName = opts.select("queue").asOpt[String]
      .orElse(config.settings.select("queue").asOpt[String])
      .getOrElse("testing")
    val routingKey = opts.select("routing_key").asOpt[String]
      .orElse(config.settings.select("routing_key").asOpt[String])
    if (env.isDev) {
      channel.queueDeclare(queueName, false, false, false, null)
    }
    routingKey match {
      case Some(routingKey) => {
        val tmpQueue = channel.queueDeclare(s"tmp-consumption-queue-${UUID.randomUUID().toString}", false, true, true, null)
        val tmpQueueName = tmpQueue.getQueue
        channel.queueBind(tmpQueueName, queueName, routingKey)
        channel.basicConsume(tmpQueueName, true, cb, cancelCb, shutdownCb)
      }
      case None => channel.basicConsume(queueName, true, cb, cancelCb, shutdownCb)
    }
    Source.fromPublisher(hotFlux)
  }
}
