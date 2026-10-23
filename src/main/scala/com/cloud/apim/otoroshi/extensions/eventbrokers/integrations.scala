package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{Outgoing, PluginUtils}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.next.workflow.{WorkflowError, WorkflowFunction}
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.*

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/** an otoroshi event (analytics, alert, audit) as a cloudevent */
object OtoroshiEvents {

  def toCloudEvent(event: JsValue): CloudEvent = {
    val kind = (event \ "@type").asOpt[String].getOrElse("event")
    CloudEvent(
      id = (event \ "@id").asOpt[String].orElse((event \ "@id").asOpt[Long].map(_.toString)).getOrElse(java.util.UUID.randomUUID().toString),
      source = s"/${(event \ "@product").asOpt[String].getOrElse("otoroshi")}/${(event \ "@env").asOpt[String].getOrElse("prod")}",
      `type` = s"io.otoroshi.$kind",
      datacontenttype = Some("application/json"),
      subject = (event \ "alert").asOpt[String].orElse((event \ "@serviceId").asOpt[String]).filter(_.nonEmpty),
      time = (event \ "@timestamp").asOpt[Long].map(CloudEvent.time).orElse(Some(CloudEvent.now())),
      data = Some(ByteString(Json.stringify(event))),
      // the events of a route stay in order, on the brokers with keys
      extensions = (event \ "@serviceId").asOpt[String].filter(_.nonEmpty).map(id => "partitionkey" -> JsString(id)).toMap
    )
  }
}

/** what the integrations (exporter, workflow functions) share: the clients of the extension */
object Integrations {

  def extension(using env: Env): Either[String, EventBrokersExtension] =
    env.adminExtensions.extension[EventBrokersExtension].toRight("the event brokers extension is not enabled")

  /**
   * publishes events through a connection: one result per event. `raw`: their data alone, without the
   * cloudevents binding of the broker (no attributes anywhere)
   */
  def publish(connection: String, events: Seq[CloudEvent], options: JsObject, raw: Boolean = false)(using
      env: Env,
      ec: ExecutionContext
  ): Future[Either[String, Seq[JsObject]]] = {
    given Materializer = env.otoroshiMaterializer
    extension.flatMap(_.connections.acquire(connection).left.map(_.message)) match {
      case Left(error)  => Future.successful(Left(error))
      case Right(lease) =>
        val published =
          if (!raw) PluginUtils.publishAll(lease.client, Source(events.toList).map(e => Right(Outgoing(e))), options).runWith(Sink.seq)
          else {
            val messages = events.map(e => BrokerMessage(key = e.extension("partitionkey").map(ByteString(_)), data = e.data.getOrElse(ByteString.empty)))
            Future.fromTry(Try(lease.client.publish(messages, options))).flatten.map(_.zip(events).map((r, e) => r.json ++ Json.obj("id" -> e.id)))
          }
        lease.releasing(published).map(Right(_)).recover { case NonFatal(e) => Left(ConnectionManager.describe(e)) }
    }
  }

  /** reads at most `take` events within `within`, acking each one as it is handed over */
  def consume(connection: String, options: JsObject, take: Int, within: FiniteDuration)(using env: Env, ec: ExecutionContext): Future[Either[String, Seq[JsObject]]] = {
    given Materializer = env.otoroshiMaterializer
    extension.flatMap(_.connections.acquire(connection).left.map(_.message)) match {
      case Left(error)  => Future.successful(Left(error))
      case Right(lease) =>
        val client = lease.client
        lease.releasing(
          PluginUtils.subscribe(client, options).flatMap {
            case Left(error)   => error.asResult().flatMap(r => r.body.consumeData.map(b => Left(s"${r.header.status}: ${b.utf8String}")))
            case Right(events) =>
              PluginUtils
                .handOver(events.take(take.toLong).takeWithin(within))(received => CloudEventBindings.fromMessage(received.message, client.config.kind).json)
                .runWith(Sink.seq)
                .map(Right(_))
          }.recover { case NonFatal(e) => Left(ConnectionManager.describe(e)) }
        )
    }
  }
}

/** `extensions.com.cloud-apim.event-brokers.publish`: publishes events from a workflow */
class EventBrokersPublishFunction extends WorkflowFunction {

  override def documentationName: String        = EventBrokersWorkflows.Publish
  override def documentationDisplayName: String = "Publish events"
  override def documentationIcon: String        = "fas fa-paper-plane"
  override def documentationCategory: Option[String] = Some("Cloud APIM - Event brokers")
  override def documentationDescription: String =
    "Publishes events through an event broker connection: `event` (a cloudevent), `events` (several), or `data` (the data of a new event)"
  override def documentationInputSchema: Option[JsObject] = Some(Json.obj(
    "type"       -> "object",
    "required"   -> Json.arr("connection"),
    "properties" -> Json.obj(
      "connection" -> Json.obj("type" -> "string", "description" -> "the id of the connection"),
      "options"    -> Json.obj("type" -> "object", "description" -> "the options of the publish: topic, queue, key, ..."),
      "event"      -> Json.obj("type" -> "object", "description" -> "a cloudevent"),
      "events"     -> Json.obj("type" -> "array", "description" -> "cloudevents"),
      "data"       -> Json.obj("description" -> "the data of a new event"),
      "type"       -> Json.obj("type" -> "string", "description" -> "the type of the new event"),
      "source"     -> Json.obj("type" -> "string", "description" -> "the source of the new event"),
      "subject"    -> Json.obj("type" -> "string", "description" -> "the subject of the new event")
    )
  ))
  override def documentationExample: Option[JsObject] = Some(Json.obj(
    "kind"     -> "call",
    "function" -> EventBrokersWorkflows.Publish,
    "args"     -> Json.obj("connection" -> "connection_xxx", "options" -> Json.obj("topic" -> "orders"), "data" -> Json.obj("id" -> 42), "type" -> "com.acme.order.created")
  ))

  private def events(args: JsObject): Either[String, Seq[CloudEvent]] =
    (args \ "event").asOpt[JsValue].map(e => CloudEvent.read(e).map(Seq(_)))
      .orElse((args \ "events").asOpt[JsValue].map(CloudEvent.readBatch))
      .orElse((args \ "data").asOpt[JsValue].map { data =>
        val base = CloudEvent.generated((args \ "source").asOpt[String].getOrElse("/otoroshi/workflows"), (args \ "type").asOpt[String].getOrElse(CloudEvent.DefaultType))
        Right(Seq(base.copy(
          subject = (args \ "subject").asOpt[String],
          datacontenttype = Some(if (data.isInstanceOf[JsString]) "text/plain" else "application/json"),
          data = Some(data match {
            case JsString(text) => ByteString(text)
            case json           => ByteString(Json.stringify(json))
          })
        )))
      })
      .getOrElse(Left("nothing to publish: give an event, events or data"))

  override def call(args: JsObject)(using env: Env, ec: ExecutionContext): Future[Either[WorkflowError, JsValue]] =
    (args \ "connection").asOpt[String] match {
      case None             => Future.successful(Left(WorkflowError("connection is required")))
      case Some(connection) =>
        events(args) match {
          case Left(error)   => Future.successful(Left(WorkflowError(error)))
          case Right(events) =>
            Integrations.publish(connection, events, (args \ "options").asOpt[JsObject].getOrElse(Json.obj())).map {
              case Left(error)    => Left(WorkflowError(error))
              case Right(results) => Right(Json.obj("results" -> results, "published" -> results.forall(r => (r \ "published").asOpt[Boolean].contains(true))))
            }
        }
    }
}

/** `extensions.com.cloud-apim.event-brokers.consume`: reads events from a workflow */
class EventBrokersConsumeFunction extends WorkflowFunction {

  override def documentationName: String        = EventBrokersWorkflows.Consume
  override def documentationDisplayName: String = "Consume events"
  override def documentationIcon: String        = "fas fa-inbox"
  override def documentationCategory: Option[String] = Some("Cloud APIM - Event brokers")
  override def documentationDescription: String =
    "Reads at most `take` events (1) within `within_ms` (5000) from an event broker connection, as cloudevents, acking each one as it is read"
  override def documentationInputSchema: Option[JsObject] = Some(Json.obj(
    "type"       -> "object",
    "required"   -> Json.arr("connection"),
    "properties" -> Json.obj(
      "connection" -> Json.obj("type" -> "string", "description" -> "the id of the connection"),
      "options"    -> Json.obj("type" -> "object", "description" -> "the options of the subscription: topic, group, from, delivery, ..."),
      "take"       -> Json.obj("type" -> "integer", "description" -> "how many events at most"),
      "within_ms"  -> Json.obj("type" -> "integer", "description" -> "how long to wait for them")
    )
  ))

  override def call(args: JsObject)(using env: Env, ec: ExecutionContext): Future[Either[WorkflowError, JsValue]] =
    (args \ "connection").asOpt[String] match {
      case None             => Future.successful(Left(WorkflowError("connection is required")))
      case Some(connection) =>
        val take   = (args \ "take").asOpt[Int].getOrElse(1).max(1).min(10000)
        val within = (args \ "within_ms").asOpt[Long].getOrElse(5000L).max(1L).min(600000L).millis
        val opts   = JsObject((args \ "options").asOpt[JsObject].getOrElse(Json.obj()).value.view.mapValues {
          // the options of a route are strings
          case JsString(s) => JsString(s)
          case other       => JsString(other.toString)
        }.toMap)
        Integrations.consume(connection, opts, take, within).map {
          case Left(error)   => Left(WorkflowError(error))
          case Right(events) => Right(Json.obj("events" -> events))
        }
    }
}

object EventBrokersWorkflows {
  val Publish = "extensions.com.cloud-apim.event-brokers.publish"
  val Consume = "extensions.com.cloud-apim.event-brokers.consume"

  def register(): Unit = {
    WorkflowFunction.registerFunction(Publish, new EventBrokersPublishFunction())
    WorkflowFunction.registerFunction(Consume, new EventBrokersConsumeFunction())
  }
}
