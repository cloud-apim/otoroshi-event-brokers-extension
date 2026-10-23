package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, EventBrokerWebhook}
import org.joda.time.DateTime
import otoroshi.env.Env
import otoroshi.events.{AlertEvent, AnalyticEvent}
import play.api.libs.json.*

private def connectionJson(connection: EventBrokerConnection): JsObject = Json.obj(
  "id"     -> connection.id,
  "name"   -> connection.name,
  "broker" -> connection.kind.name
)

/**
 * Sent each time a connection changes state on a node (connected, unavailable, closed), so data
 * exporters can follow the brokers. Every node sends its own: each one has its own clients.
 */
final case class EventBrokerConnectionEvent(
    connection: EventBrokerConnection,
    previous: ConnectionStatus,
    current: ConnectionStatus,
    env: Env
) extends AnalyticEvent {

  val `@id`: String                          = env.snowflakeGenerator.nextIdStr()
  val `@timestamp`: DateTime                 = DateTime.now()
  override def `@type`: String               = "EventBrokerConnectionEvent"
  override def `@service`: String            = connection.name
  override def `@serviceId`: String          = connection.id
  override def fromOrigin: Option[String]    = None
  override def fromUserAgent: Option[String] = None

  override def toJson(using _env: Env): JsValue = Json.obj(
    "@id"            -> `@id`,
    "@timestamp"     -> play.api.libs.json.JodaWrites.JodaDateTimeNumberWrites.writes(`@timestamp`),
    "@type"          -> `@type`,
    "@product"       -> _env.eventsName,
    "@serviceId"     -> `@serviceId`,
    "@service"       -> `@service`,
    "@env"           -> _env.env,
    "connection"     -> connectionJson(connection),
    "previous_state" -> previous.state.name,
    "state"          -> current.state.name,
    "status"         -> current.json
  )
}

/** Sent when a connection becomes unavailable on a node: it could not connect, or lost its broker. */
final case class EventBrokerConnectionUnavailableAlert(
    connection: EventBrokerConnection,
    previous: ConnectionStatus,
    current: ConnectionStatus,
    env: Env
) extends AlertEvent {

  val `@id`: String                          = env.snowflakeGenerator.nextIdStr()
  val `@timestamp`: DateTime                 = DateTime.now()
  override def `@service`: String            = connection.name
  override def `@serviceId`: String          = connection.id
  override def fromOrigin: Option[String]    = None
  override def fromUserAgent: Option[String] = None

  override def toJson(using _env: Env): JsValue = Json.obj(
    "@id"            -> `@id`,
    "@timestamp"     -> play.api.libs.json.JodaWrites.JodaDateTimeNumberWrites.writes(`@timestamp`),
    "@type"          -> `@type`,
    "@product"       -> _env.eventsName,
    "@serviceId"     -> `@serviceId`,
    "@service"       -> `@service`,
    "@env"           -> _env.env,
    "alert"          -> "EventBrokerConnectionUnavailable",
    "connection"     -> connectionJson(connection),
    // connecting: it never worked with these settings. connected: it lost its broker
    "previous_state" -> previous.state.name,
    "error"          -> current.error,
    "status"         -> current.json
  )
}

/** Sent when a webhook gives an event up: refused by its endpoint, or failed every attempt, and no dead letters took it. */
final case class EventBrokerWebhookDeliveryFailedAlert(webhook: EventBrokerWebhook, event: CloudEvent, attempts: Int, reason: String, env: Env) extends AlertEvent {

  val `@id`: String                          = env.snowflakeGenerator.nextIdStr()
  val `@timestamp`: DateTime                 = DateTime.now()
  override def `@service`: String            = webhook.name
  override def `@serviceId`: String          = webhook.id
  override def fromOrigin: Option[String]    = None
  override def fromUserAgent: Option[String] = None

  override def toJson(using _env: Env): JsValue = Json.obj(
    "@id"        -> `@id`,
    "@timestamp" -> play.api.libs.json.JodaWrites.JodaDateTimeNumberWrites.writes(`@timestamp`),
    "@type"      -> `@type`,
    "@product"   -> _env.eventsName,
    "@serviceId" -> `@serviceId`,
    "@service"   -> `@service`,
    "@env"       -> _env.env,
    "alert"      -> "EventBrokerWebhookDeliveryFailed",
    "webhook"    -> Json.obj("id" -> webhook.id, "name" -> webhook.name, "connection" -> webhook.connection, "url" -> webhook.endpoint.url),
    "event"      -> Json.obj("id" -> event.id, "source" -> event.source, "type" -> event.`type`),
    "attempts"   -> attempts,
    "reason"     -> reason
  )
}
