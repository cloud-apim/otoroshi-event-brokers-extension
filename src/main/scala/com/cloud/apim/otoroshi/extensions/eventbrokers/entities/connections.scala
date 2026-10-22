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

enum EventBrokerConnectionKind(val name: String) {
  case Kafka    extends EventBrokerConnectionKind("kafka")
  case RabbitMQ extends EventBrokerConnectionKind("rabbitmq")
  case Pulsar   extends EventBrokerConnectionKind("pulsar")
  case Redis    extends EventBrokerConnectionKind("redis")
  case MQTT     extends EventBrokerConnectionKind("mqtt")
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

  def hash: String = s"${id}-${settings.stringify}".sha256

  /** a new client, not started: the ConnectionManager owns its lifecycle */
  def newClientInstance()(using env: Env): EventBrokerClient = kind match {
    case EventBrokerConnectionKind.Kafka => new KafkaBrokerClient(this, env)
    case EventBrokerConnectionKind.Pulsar => new PulsarBrokerClient(this)
    case EventBrokerConnectionKind.Redis => new RedisBrokerClient(this)
    case EventBrokerConnectionKind.MQTT => new MQTTBrokerClient(this)
    case EventBrokerConnectionKind.RabbitMQ => new RabbitMqBrokerClient(this)
  }
}

object EventBrokerConnection {
  given format: Format[EventBrokerConnection] = new Format[EventBrokerConnection] {
    override def writes(o: EventBrokerConnection): JsValue             = o.location.jsonWithKey ++ Json.obj(
      "id"          -> o.id,
      "name"        -> o.name,
      "description" -> o.description,
      "metadata"    -> o.metadata,
      "tags"        -> JsArray(o.tags.map(JsString.apply)),
      "kind"        -> o.kind.json,
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
        kind = (json \ "kind").asOpt[String].flatMap(EventBrokerConnectionKind.apply).getOrElse(EventBrokerConnectionKind.Kafka),
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
      GenericResourceAccessApiWithState[EventBrokerConnection](
        format = EventBrokerConnection.format ,
        clazz = classOf[EventBrokerConnection],
        keyf = id => datastores.eventBrokerConnectionDatastore.key(id),
        extractIdf = c => datastores.eventBrokerConnectionDatastore.extractId(c),
        extractIdJsonf = json => json.select("id").asString,
        idFieldNamef = () => "id",
        tmpl = (_, p, _) => {
          p.get("kind").map(_.toLowerCase()) match {
            case Some("rabbitmq") => EventBrokerConnection(
              id = IdGenerator.namedId("provider", env),
              name = "RabbitMQ connection",
              description = "A RabbitMQ connection",
              metadata = Map.empty,
              tags = Seq.empty,
              location = EntityLocation.default,
              kind = EventBrokerConnectionKind.RabbitMQ,
              settings = Json.obj(),
            ).json
            case _ => EventBrokerConnection(
              id = IdGenerator.namedId("provider", env),
              name = "Kafka connection",
              description = "A Kafka connection",
              metadata = Map.empty,
              tags = Seq.empty,
              location = EntityLocation.default,
              kind = EventBrokerConnectionKind.Kafka,
              settings = Json.obj(),
            ).json
          }

        },
        canRead = true,
        canCreate = true,
        canUpdate = true,
        canDelete = true,
        canBulk = true,
        stateAll = () => states.allConnections(),
        stateOne = id => states.connection(id),
        stateUpdate = values => states.updateConnections(values)
      )
    )
  }
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
