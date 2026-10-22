package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, RabbitMQSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import com.rabbitmq.client._
import otoroshi.env.Env
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.{JsObject, JsValue}
import reactor.core.publisher.Sinks

import java.util.concurrent.atomic.AtomicReference
import java.util.{Date, UUID}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

class RabbitMqBrokerClient(val config: EventBrokerConnection, settings: RabbitMQSettings) extends EventBrokerClient {

  val factory = new ConnectionFactory()
  factory.setVirtualHost(settings.virtualHost)
  factory.setTopologyRecoveryEnabled(false)
  factory.setAutomaticRecoveryEnabled(true)
  factory.setNetworkRecoveryInterval(1000)
  settings.username.foreach(v => factory.setUsername(v))
  settings.password.foreach(v => factory.setPassword(v))
  // TODO: handle declarations
  // TODO: handle recovery
  factory.setConnectionTimeout(10000)
  private val servers = settings.addresses

  private val connectionRef = new AtomicReference[Option[Connection]](None)
  private val channelRef    = new AtomicReference[Option[Channel]](None)

  private def channel: Channel = channelRef.get().getOrElse(throw new IllegalStateException("the rabbitmq client is not started"))

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    val connection = factory.newConnection(servers.map(s => Address.parseAddress(s)).toArray)
    connectionRef.set(Some(connection))
    channelRef.set(Some(connection.createChannel()))
  }

  // the connection recovers by itself after a network failure, the channel does not survive a
  // protocol error (a publish to an exchange that does not exist closes it)
  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = {
    if (!connectionRef.get().exists(_.isOpen)) Future.failed(new IllegalStateException("the rabbitmq connection is closed"))
    else if (!channelRef.get().exists(_.isOpen)) Future.failed(new IllegalStateException("the rabbitmq channel is closed"))
    else Future.unit
  }

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    channelRef.getAndSet(None).foreach(c => Try(if (c.isOpen) c.close()))
    connectionRef.getAndSet(None).foreach(c => Try(c.close(5000)))
  }

  def buildProps(wrapper: EventWrapper): AMQP.BasicProperties = {
    val headers: java.util.Map[String, AnyRef] = (wrapper.headers ++ Map("content-type" -> wrapper.dataContentType, "key-content-type" -> wrapper.keyContentType)).view.mapValues(_.asInstanceOf[AnyRef]).toMap.asJava
    new AMQP.BasicProperties.Builder()
      .headers(headers)
      .applyOnWithOpt(wrapper.vendorMetadata.get("contentType")) { case (builder, value) => builder.contentType(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("contentEncoding")) { case (builder, value) => builder.contentEncoding(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("deliveryMode")) { case (builder, value) => builder.deliveryMode(value.toInt) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("priority")) { case (builder, value) => builder.priority(value.toInt) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("correlationId")) { case (builder, value) => builder.correlationId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("replyTo")) { case (builder, value) => builder.replyTo(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("expiration")) { case (builder, value) => builder.expiration(value) }
      .applyOnWithOpt(wrapper.key.map(_.utf8String).orElse(wrapper.vendorMetadata.get("messageId"))) { case (builder, value) => builder.messageId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("timestamp")) { case (builder, value) => builder.timestamp(new Date(value.toLong)) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("type")) { case (builder, value) => builder.`type`(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("userId")) { case (builder, value) => builder.userId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("appId")) { case (builder, value) => builder.appId(value) }
      .applyOnWithOpt(wrapper.vendorMetadata.get("clusterId")) { case (builder, value) => builder.clusterId(value) }
      .build()
  }

  override def publish(event: EventWrapper, opts: JsObject, contentType: String)(using env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val queueName = event.vendorMetadata.get("queue")
      .orElse(opts.select("queue").asOpt[String])
      .orElse(settings.defaultQueue)
      .getOrElse(throw new IllegalArgumentException("no queue: none in the call, and the connection has no default_queue"))
    val routingKey = event.vendorMetadata.get("routing_key")
      .orElse(opts.select("routing_key").asOpt[String])
    val props = buildProps(event)
    if (env.isDev) {
      channel.queueDeclare(queueName, false, false, false, null)
    }
    routingKey match {
      case Some(routingKey) => channel.basicPublish(queueName, routingKey, props, event.data.toArray)
      case None => channel.basicPublish("", queueName, props, event.data.toArray)
    }
    Right(None).vfuture
  }

  override def consume(opts: JsObject)(using env: Env, ec: ExecutionContext): Source[EventWrapper, ?] = {
    val hotSource = Sinks.many().unicast().onBackpressureBuffer[EventWrapper]()
    val hotFlux   = hotSource.asFlux()
    val cb = new DeliverCallback() {
      override def handle(consumerTag: String, message: Delivery): Unit = {
        val data = ByteString(message.getBody)
        val props = Option(message.getProperties)
        val envelope = message.getEnvelope
        val headers: Map[String, AnyRef] = props.map(_.getHeaders).map(_.asScala.toMap).getOrElse(Map.empty[String, AnyRef])
        val contentType = headers.get("content-type").map(_.toString).orElse(props.map(_.getContentType)).filter(_ != null).getOrElse("binary")
        hotSource.tryEmitNext(
          EventWrapper(
            key = props.flatMap(v => Option(v.getMessageId)).map(_.byteString),
            data = data,
            keyContentType = "text",
            dataContentType = contentType,
            headers = headers.collect {
              case (key, value) if value != null => (key, value.toString)
            },
            vendorMetadata = Map.apply[String, String](
                "provider" -> "rabbitmq",
                "exchange" -> envelope.getExchange,
                "routing_key" -> envelope.getRoutingKey,
                "delivery_tag" -> envelope.getDeliveryTag.toString,
              )
              .applyOnWithOpt(props) { case (map, props) => if (props.getContentType != null) map ++ Map("content_type" -> props.getContentType) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getContentEncoding != null) map ++ Map("content_encoding" -> props.getContentEncoding) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getDeliveryMode != null) map ++ Map("delivery_mode" -> Option(props.getDeliveryMode).map(_.toString).getOrElse("")) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getPriority != null) map ++ Map("priority" -> Option(props.getPriority).map(_.toString).getOrElse("")) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getCorrelationId != null) map ++ Map("correlation_id" -> props.getCorrelationId) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getReplyTo != null) map ++ Map("reply_to" -> props.getReplyTo) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getExpiration != null) map ++ Map("expiration" -> props.getExpiration) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getMessageId != null) map ++ Map("message_id" -> props.getMessageId) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getTimestamp != null) map ++ Map("timestamp" -> Option(props.getTimestamp).map(_.toString).getOrElse("")) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getType != null) map ++ Map("type" -> props.getType) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getUserId != null) map ++ Map("UserId" -> props.getUserId) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getAppId != null) map ++ Map("app_id" -> props.getAppId) else map }
              .applyOnWithOpt(props) { case (map, props) => if (props.getClusterId != null) map ++ Map("cluster_id" -> props.getClusterId) else map }
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
      .orElse(settings.defaultQueue)
      .getOrElse(throw new IllegalArgumentException("no queue: none in the call, and the connection has no default_queue"))
    val routingKey = opts.select("routing_key").asOpt[String]
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
