package com.cloud.apim.otoroshi.extensions.eventbrokers.entities

import com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokerClient
import com.cloud.apim.otoroshi.extensions.eventbrokers.clients._
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

enum EventBrokerConnectionKind(val name: String, val label: String) {
  case Kafka    extends EventBrokerConnectionKind("kafka", "Kafka")
  case RabbitMQ extends EventBrokerConnectionKind("rabbitmq", "RabbitMQ")
  case Pulsar   extends EventBrokerConnectionKind("pulsar", "Pulsar")
  case Redis    extends EventBrokerConnectionKind("redis", "Redis")
  case MQTT     extends EventBrokerConnectionKind("mqtt", "MQTT")
  case Artemis  extends EventBrokerConnectionKind("artemis", "ActiveMQ Artemis")
  def json: JsValue = name.json
}

object EventBrokerConnectionKind {
  def apply(str: String): Option[EventBrokerConnectionKind] = {
    val lower = str.toLowerCase
    values.find(_.name == lower)
  }
}

case class EventBrokerConnection(
   location: EntityLocation,
   id: String,
   name: String,
   description: String,
   tags: Seq[String],
   metadata: Map[String, String],
   kind: EventBrokerConnectionKind,
   settings: JsObject,
) extends EntityLocationSupport {

  override def internalId: String               = id
  override def json: JsValue                    = EventBrokerConnection.format.writes(this)
  override def theName: String                  = name
  override def theDescription: String           = description
  override def theTags: Seq[String]             = tags
  override def theMetadata: Map[String, String] = metadata

  def hash: String = s"${id}-${kind.name}-${settings.stringify}".sha256

  /**
   * the settings, read for the kind of the connection. kept raw in the entity: a connection whose
   * settings are wrong (written by an older version, by hand in the datastore) must stay visible and
   * fixable, writes through the admin api are checked
   */
  lazy val typedSettings: Either[Seq[String], BrokerSettings] = BrokerSettings.read(kind, settings)

  /** a new client, not started: the ConnectionManager owns its lifecycle */
  def newClientInstance()(using env: Env): EventBrokerClient = typedSettings match {
    case Left(errors)               => throw new InvalidConnectionSettings(errors)
    case Right(s: KafkaSettings)    => new KafkaBrokerClient(this, s, env)
    case Right(s: PulsarSettings)   => new PulsarBrokerClient(this, s)
    case Right(s: RedisSettings)    => if (s.isStreams) new RedisStreamsBrokerClient(this, s) else new RedisBrokerClient(this, s)
    case Right(s: ArtemisSettings)  => new ArtemisBrokerClient(this, s)
    case Right(s: MqttSettings)     => new MQTTBrokerClient(this, s)
    case Right(s: RabbitMQSettings) => new RabbitMqBrokerClient(this, s)
  }
}

object EventBrokerConnection {

  /**
   * the broker of a connection, in its json. `broker`, and not `kind`: otoroshi's admin api writes the
   * kind of the resource (`event-brokers.extensions.cloud-apim.com/EventBrokerConnection`) in `kind`
   * on everything it serves. a `kind` naming a broker is still read (written before)
   */
  def brokerOf(json: JsValue): Option[String] =
    (json \ "broker").asOpt[String].orElse((json \ "kind").asOpt[String].filter(k => EventBrokerConnectionKind(k).isDefined))

  /** what the admin api refuses to write: an unknown broker, settings that do not fit it */
  def validate(entity: EventBrokerConnection, body: JsValue): Either[Seq[String], EventBrokerConnection] = {
    val kindErrors = brokerOf(body) match {
      case None                                                     => Seq("broker: required")
      case Some(broker) if EventBrokerConnectionKind(broker).isEmpty =>
        Seq(s"broker: must be one of ${EventBrokerConnectionKind.values.map(_.name).mkString(", ")}")
      case _                                                        => Seq.empty
    }
    val settingsErrors = if (kindErrors.nonEmpty) Seq.empty else entity.typedSettings.left.toOption.toSeq.flatten.map(e => s"settings.$e")
    val errors         = kindErrors ++ settingsErrors
    if (errors.isEmpty) Right(entity) else Left(errors)
  }

  def template(kind: EventBrokerConnectionKind, env: Env): EventBrokerConnection = EventBrokerConnection(
    location = EntityLocation.default,
    id = IdGenerator.namedId("connection", env),
    name = s"${kind.label} connection",
    description = s"A ${kind.label} connection",
    tags = Seq.empty,
    metadata = Map.empty,
    kind = kind,
    settings = BrokerSettings.template(kind).json
  )

  given format: Format[EventBrokerConnection] = new Format[EventBrokerConnection] {
    override def writes(o: EventBrokerConnection): JsValue             = o.location.jsonWithKey ++ Json.obj(
      "id"          -> o.id,
      "name"        -> o.name,
      "description" -> o.description,
      "metadata"    -> o.metadata,
      "tags"        -> JsArray(o.tags.map(JsString.apply)),
      "broker"      -> o.kind.json,
      "settings"    -> o.settings,
    )
    override def reads(json: JsValue): JsResult[EventBrokerConnection] = Try {
      EventBrokerConnection(
        location = otoroshi.models.EntityLocation.readFromKey(json),
        id = (json \ "id").as[String],
        name = (json \ "name").as[String],
        description = (json \ "description").as[String],
        metadata = (json \ "metadata").asOpt[Map[String, String]].getOrElse(Map.empty),
        tags = (json \ "tags").asOpt[Seq[String]].getOrElse(Seq.empty[String]),
        kind = brokerOf(json).flatMap(EventBrokerConnectionKind.apply).getOrElse(EventBrokerConnectionKind.Kafka),
        settings = (json \ "settings").asOpt[JsObject].getOrElse(Json.obj()),
      )
    } match {
      case Failure(ex)    => JsError(ex.getMessage)
      case Success(value) => JsSuccess(value)
    }
  }
  def resource(env: Env, datastores: EventBrokersExtensionDatastores, states: EventBrokersExtensionState): Resource = {
    Resource(
      "EventBrokerConnection",
      "event-broker-connections",
      "event-broker-connection",
      "event-brokers.extensions.cloud-apim.com",
      ResourceVersion("v1", true, false, true),
      GenericResourceAccessApiWithStateAndWriteValidation[EventBrokerConnection](
        format = EventBrokerConnection.format ,
        clazz = classOf[EventBrokerConnection],
        keyf = id => datastores.eventBrokerConnectionDatastore.key(id),
        extractIdf = c => datastores.eventBrokerConnectionDatastore.extractId(c),
        extractIdJsonf = json => json.select("id").asString,
        idFieldNamef = () => "id",
        // ?broker=pulsar for the template of a broker, kafka without
        tmpl = (_, p, _) => template(p.get("broker").orElse(p.get("kind")).flatMap(EventBrokerConnectionKind.apply).getOrElse(EventBrokerConnectionKind.Kafka), env).json,
        canRead = true,
        canCreate = true,
        canUpdate = true,
        canDelete = true,
        canBulk = true,
        stateAll = () => states.allConnections(),
        stateOne = id => states.connection(id),
        stateUpdate = values => states.updateConnections(values),
        writeValidator = (entity, body, _, _, _, _, _) => checked(entity, body),
        writeChecker = (entity, body, _, _, _, _, _) => checked(entity, body)
      )
    )
  }
}

private def checked(entity: EventBrokerConnection, body: JsValue): scala.concurrent.Future[Either[JsValue, EventBrokerConnection]] =
  EventBrokerConnection.validate(entity, body) match {
    case Right(valid)  => scala.concurrent.Future.successful(Right(valid))
    case Left(errors)  =>
      scala.concurrent.Future.successful(
        Left(Json.obj("error" -> "bad_request", "error_description" -> errors.mkString("; "), "errors" -> errors, "http_status_code" -> 400))
      )
  }

trait EventBrokerConnectionDatastore extends BasicStore[EventBrokerConnection]

class KvEventBrokerConnectionDatastore(extensionId: AdminExtensionId, redisCli: RedisLike, _env: Env)
  extends EventBrokerConnectionDatastore
    with RedisLikeStore[EventBrokerConnection] {
  override def fmt: Format[EventBrokerConnection]   = EventBrokerConnection.format
  override def redisLike(using env: Env): RedisLike = redisCli
  override def key(id: String): String              = s"${_env.storageRoot}:extensions:${extensionId.cleanup}:connections:$id"
  override def extractId(value: EventBrokerConnection): String = value.id
}
