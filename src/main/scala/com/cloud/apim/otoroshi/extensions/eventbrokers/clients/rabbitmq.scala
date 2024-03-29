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

import java.util.{Date, UUID}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

class RabbitMqBrokerClient(val config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  val factory = new ConnectionFactory()
  factory.setVirtualHost(config.settings.select("virtual_host").asOpt[String].getOrElse("/"))
  factory.setTopologyRecoveryEnabled(false)
  factory.setAutomaticRecoveryEnabled(true)
  factory.setNetworkRecoveryInterval(1000)
  config.settings.select("username").asOpt[String].filterNot(_.isBlank).foreach(v => factory.setUsername(v))
  config.settings.select("password").asOpt[String].filterNot(_.isBlank).foreach(v => factory.setPassword(v))
  // TODO: handle declarations
  // TODO: handle recovery
  val servers = config.settings.select("servers").asOpt[Seq[String]].getOrElse(Seq.empty)
  val connection = factory.newConnection(servers.map(s => Address.parseAddress(s)).toArray)
  val channel = connection.createChannel()

  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    channel.close()
    connection.close()
    ().vfuture
  }

  def buildProps(wrapper: EventWrapper, opts: JsObject): AMQP.BasicProperties = {
    val headers: java.util.Map[String, AnyRef] = (wrapper.headers ++ Map("content-type" -> wrapper.dataContentType, "key-content-type" -> wrapper.keyContentType)).mapValues(_.asInstanceOf[AnyRef]).asJava
    new AMQP.BasicProperties.Builder()
      .headers(headers)
      .applyOnWithOpt(wrapper.vendorMetadata.get("contentType")) { case (builder, value) => builder.contentType(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("contentEncoding")) { case (builder, value) => builder.contentEncoding(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("deliveryMode")) { case (builder, value) => builder.deliveryMode(value.toInt) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("priority")) { case (builder, value) => builder.priority(value.toInt) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("correlationId")) { case (builder, value) => builder.correlationId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("replyTo")) { case (builder, value) => builder.replyTo(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("expiration")) { case (builder, value) => builder.expiration(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("messageId")) { case (builder, value) => builder.messageId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("timestamp")) { case (builder, value) => builder.timestamp(new Date(value.toLong)) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("type")) { case (builder, value) => builder.`type`(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("userId")) { case (builder, value) => builder.userId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("appId")) { case (builder, value) => builder.appId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("clusterId")) { case (builder, value) => builder.clusterId(value) }
      .build()
  }

  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    // println(s"publish to '${queueName}' - ${event.utf8String}")
    val queueName = event.vendorMetadata.get("queue")
      .orElse(opts.select("queue").asOpt[String])
      .orElse(config.settings.select("queue").asOpt[String])
      .getOrElse("testing")
    val routingKey = event.vendorMetadata.get("routing_key")
      .orElse(opts.select("routing_key").asOpt[String])
      .orElse(config.settings.select("routing_key").asOpt[String])
    val props = buildProps(event, opts)
    if (env.isDev) {
      channel.queueDeclare(queueName, false, false, false, null)
    }
    routingKey match {
      case Some(routingKey) => channel.basicPublish(queueName, routingKey, props, event.data.toArray)
      case None => channel.basicPublish("", queueName, props, event.data.toArray)
    }
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val hotSource = Sinks.many().unicast().onBackpressureBuffer[EventWrapper]()
    val hotFlux   = hotSource.asFlux()
    val cb = new DeliverCallback() {
      override def handle(consumerTag: String, message: Delivery): Unit = {
        val data = ByteString(message.getBody)
        val props = message.getProperties
        val envelope = message.getEnvelope
        val headers = props.getHeaders
        // println(s"received to '${queueName}' - ${data.utf8String}")
        val contentType = Option(props.getContentType).getOrElse("binary")
        hotSource.tryEmitNext(
          EventWrapper(
            key = Option(props.getMessageId).map(_.byteString),
            data = data,
            keyContentType = "text",
            dataContentType = contentType,
            headers = headers.asScala.collect {
              case (key, value) if value != null => (key, value.toString)
            }.toMap,
            vendorMetadata = Map.apply[String, String](
              "provider" -> "rabbitmq",
              "exchange" -> envelope.getExchange,
              "routing_key" -> envelope.getRoutingKey,
              "delivery_tag" -> envelope.getDeliveryTag.toString,
              "content_type" -> props.getContentType().toString,
              "content_encoding" -> props.getContentEncoding().toString,
              "delivery_mode" -> props.getDeliveryMode().toString,
              "priority" -> props.getPriority().toString,
              "correlation_id" -> props.getCorrelationId().toString,
              "reply_to" -> props.getReplyTo().toString,
              "expiration" -> props.getExpiration().toString,
              "message_id" -> props.getMessageId().toString,
              "timestamp" -> props.getTimestamp().toString,
              "Type" -> props.getType().toString,
              "UserId" -> props.getUserId().toString,
              "app_id" -> props.getAppId().toString,
              "cluster_id" -> props.getClusterId().toString,
            )
          )
        )
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
