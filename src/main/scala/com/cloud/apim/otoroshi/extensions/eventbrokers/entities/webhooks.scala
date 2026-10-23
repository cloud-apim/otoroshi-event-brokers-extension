package com.cloud.apim.otoroshi.extensions.eventbrokers.entities

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.TlsSettings
import otoroshi.api._
import otoroshi.env.Env
import otoroshi.models._
import otoroshi.next.extensions._
import otoroshi.security.IdGenerator
import otoroshi.storage._
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokersExtensionDatastores, EventBrokersExtensionState}
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

/**
 * Where a webhook delivers: `mode` is how an event goes over http (the cloudevents http binding):
 * `structured` (the whole event as `application/cloudevents+json`), `binary` (the attributes in `ce-`
 * headers, the data as the body) or `raw` (the data alone). With a `secret`, every body is signed
 * (hmac-sha256, in `X-Event-Brokers-Signature: sha256=<hex>`).
 */
final case class WebhookEndpoint(
    url: String,
    method: String = "POST",
    headers: Map[String, String] = Map.empty,
    timeoutMs: Long = 10000L,
    mode: String = "structured",
    secret: Option[String] = None,
    tls: TlsSettings = TlsSettings()
) {
  def json: JsObject = Json.obj(
    "url"        -> url,
    "method"     -> method,
    "headers"    -> headers,
    "timeout_ms" -> timeoutMs,
    "mode"       -> mode,
    "secret"     -> secret,
    "tls"        -> tls.json
  )
}

/** a delivery that fails is tried again, waiting twice as long each time, `max_attempts` times in all */
final case class WebhookRetry(maxAttempts: Int = 5, initialBackoffMs: Long = 1000L, maxBackoffMs: Long = 30000L) {
  def json: JsObject = Json.obj("max_attempts" -> maxAttempts, "initial_backoff_ms" -> initialBackoffMs, "max_backoff_ms" -> maxBackoffMs)
}

/** where an event goes once its delivery failed for good: a connection (the webhook's one by default) and the options of the publish */
final case class WebhookDeadLetter(connection: Option[String] = None, options: JsObject = Json.obj()) {
  def json: JsObject = Json.obj("connection" -> connection, "options" -> options)
}

/**
 * Events of a connection pushed to an http endpoint. `subscription` holds the options of the
 * subscription, the ones a consumer route takes (`topic`, `group`, `from`, `delivery`, `selector`,
 * ...). One node at a time delivers a webhook.
 */
case class EventBrokerWebhook(
    location: EntityLocation,
    id: String,
    name: String,
    description: String,
    tags: Seq[String],
    metadata: Map[String, String],
    enabled: Boolean = true,
    connection: String,
    subscription: JsObject = Json.obj(),
    endpoint: WebhookEndpoint,
    retry: WebhookRetry = WebhookRetry(),
    /** deliveries at once: 1 keeps the order of the events */
    maxInFlight: Int = 1,
    deadLetter: Option[WebhookDeadLetter] = None
) extends EntityLocationSupport {

  override def internalId: String               = id
  override def json: JsValue                    = EventBrokerWebhook.format.writes(this)
  override def theName: String                  = name
  override def theDescription: String           = description
  override def theTags: Seq[String]             = tags
  override def theMetadata: Map[String, String] = metadata

  /** what makes a webhook deliver differently: a change restarts its delivery */
  def hash: String = Json.stringify(json.as[JsObject] - "name" - "description" - "tags" - "metadata").sha256
}

object EventBrokerWebhook {

  val modes: Seq[String]   = Seq("structured", "binary", "raw")
  val methods: Seq[String] = Seq("POST", "PUT", "PATCH")

  /** reads the settings of a webhook field by field, every error with its path */
  def read(json: JsObject): Either[Seq[String], EventBrokerWebhook] = Fields.read(json) { f =>
    f.allow("_loc", "id", "name", "description", "tags", "metadata", "kind")
    val endpoint = f.obj("endpoint") { e =>
      val url     = e.requiredString("url").getOrElse("")
      e.check("url", url.isEmpty || url.startsWith("http://") || url.startsWith("https://"), "must be an http:// or https:// url")
      val timeout = e.get[Long]("timeout_ms", 10000L)
      if (timeout < 1) e.error("timeout_ms", "must be at least 1")
      WebhookEndpoint(
        url = url,
        method = e.oneOf("method", "POST", methods),
        headers = e.get[Map[String, String]]("headers", Map.empty),
        timeoutMs = timeout,
        mode = e.oneOf("mode", "structured", modes),
        secret = e.optString("secret"),
        tls = e.obj("tls")(TlsSettings.readFields)
      )
    }
    val retry    = f.obj("retry") { r =>
      val attempts = r.intIn("max_attempts", 5, 1, 1000)
      val initial  = r.get[Long]("initial_backoff_ms", 1000L)
      val max      = r.get[Long]("max_backoff_ms", 30000L)
      if (initial < 0) r.error("initial_backoff_ms", "must not be negative")
      if (max < initial) r.error("max_backoff_ms", "must be at least initial_backoff_ms")
      WebhookRetry(attempts, initial, max)
    }
    val dead     = (json \ "dead_letter").toOption.filter(_ != JsNull).map(_ => f.obj("dead_letter")(d => WebhookDeadLetter(d.optString("connection"), d.get[JsObject]("options", Json.obj()))))
    if (dead.isEmpty) f.allow("dead_letter")
    EventBrokerWebhook(
      location = EntityLocation.readFromKey(json),
      id = (json \ "id").asOpt[String].getOrElse(""),
      name = (json \ "name").asOpt[String].getOrElse(""),
      description = (json \ "description").asOpt[String].getOrElse(""),
      tags = (json \ "tags").asOpt[Seq[String]].getOrElse(Seq.empty),
      metadata = (json \ "metadata").asOpt[Map[String, String]].getOrElse(Map.empty),
      enabled = f.get[Boolean]("enabled", true),
      connection = f.requiredString("connection").getOrElse(""),
      subscription = f.get[JsObject]("subscription", Json.obj()),
      endpoint = endpoint,
      retry = retry,
      maxInFlight = f.intIn("max_in_flight", 1, 1, 1000),
      deadLetter = dead
    )
  }

  def template(env: Env): EventBrokerWebhook = EventBrokerWebhook(
    location = EntityLocation.default,
    id = IdGenerator.namedId("webhook", env),
    name = "New webhook",
    description = "Events pushed to an http endpoint",
    tags = Seq.empty,
    metadata = Map.empty,
    connection = "",
    subscription = Json.obj("topic" -> "events"),
    endpoint = WebhookEndpoint(url = "https://example.com/events")
  )

  given format: Format[EventBrokerWebhook] = new Format[EventBrokerWebhook] {
    override def writes(o: EventBrokerWebhook): JsValue = o.location.jsonWithKey ++ Json.obj(
      "id"            -> o.id,
      "name"          -> o.name,
      "description"   -> o.description,
      "metadata"      -> o.metadata,
      "tags"          -> JsArray(o.tags.map(JsString.apply)),
      "enabled"       -> o.enabled,
      "connection"    -> o.connection,
      "subscription"  -> o.subscription,
      "endpoint"      -> o.endpoint.json,
      "retry"         -> o.retry.json,
      "max_in_flight" -> o.maxInFlight,
      "dead_letter"   -> o.deadLetter.map(_.json)
    )
    // a stored webhook stays readable whatever it holds: what is wrong falls back to the defaults
    override def reads(json: JsValue): JsResult[EventBrokerWebhook] = Try {
      json.as[JsObject] match {
        case obj =>
          read(obj).getOrElse(
            EventBrokerWebhook(
              location = EntityLocation.readFromKey(json),
              id = (json \ "id").as[String],
              name = (json \ "name").asOpt[String].getOrElse(""),
              description = (json \ "description").asOpt[String].getOrElse(""),
              tags = (json \ "tags").asOpt[Seq[String]].getOrElse(Seq.empty),
              metadata = (json \ "metadata").asOpt[Map[String, String]].getOrElse(Map.empty),
              // not delivered: what it says is not valid
              enabled = false,
              connection = (json \ "connection").asOpt[String].getOrElse(""),
              endpoint = WebhookEndpoint(url = (json \ "endpoint" \ "url").asOpt[String].getOrElse(""))
            )
          )
      }
    } match {
      case Failure(ex)    => JsError(ex.getMessage)
      case Success(value) => JsSuccess(value)
    }
  }

  private def checked(body: JsValue): scala.concurrent.Future[Either[JsValue, EventBrokerWebhook]] =
    scala.concurrent.Future.successful(
      body.asOpt[JsObject].map(read).getOrElse(Left(Seq("expected an object"))) match {
        case Right(valid) => Right(valid)
        case Left(errors) => Left(Json.obj("error" -> "bad_request", "error_description" -> errors.mkString("; "), "errors" -> errors, "http_status_code" -> 400))
      }
    )

  def resource(env: Env, datastores: EventBrokersExtensionDatastores, states: EventBrokersExtensionState): Resource = {
    Resource(
      "EventBrokerWebhook",
      "event-broker-webhooks",
      "event-broker-webhook",
      "event-brokers.extensions.cloud-apim.com",
      ResourceVersion("v1", true, false, true),
      GenericResourceAccessApiWithStateAndWriteValidation[EventBrokerWebhook](
        format = EventBrokerWebhook.format,
        clazz = classOf[EventBrokerWebhook],
        keyf = id => datastores.eventBrokerWebhookDatastore.key(id),
        extractIdf = c => datastores.eventBrokerWebhookDatastore.extractId(c),
        extractIdJsonf = json => json.select("id").asString,
        idFieldNamef = () => "id",
        tmpl = (_, _, _) => template(env).json,
        canRead = true,
        canCreate = true,
        canUpdate = true,
        canDelete = true,
        canBulk = true,
        stateAll = () => states.allWebhooks(),
        stateOne = id => states.webhook(id),
        stateUpdate = values => states.updateWebhooks(values),
        writeValidator = (_, body, _, _, _, _, _) => checked(body),
        writeChecker = (_, body, _, _, _, _, _) => checked(body)
      )
    )
  }
}

trait EventBrokerWebhookDatastore extends BasicStore[EventBrokerWebhook]

class KvEventBrokerWebhookDatastore(extensionId: AdminExtensionId, redisCli: RedisLike, _env: Env)
    extends EventBrokerWebhookDatastore
    with RedisLikeStore[EventBrokerWebhook] {
  override def fmt: Format[EventBrokerWebhook]      = EventBrokerWebhook.format
  override def redisLike(using env: Env): RedisLike = redisCli
  override def key(id: String): String              = s"${_env.storageRoot}:extensions:${extensionId.cleanup}:webhooks:$id"
  override def extractId(value: EventBrokerWebhook): String = value.id
}
