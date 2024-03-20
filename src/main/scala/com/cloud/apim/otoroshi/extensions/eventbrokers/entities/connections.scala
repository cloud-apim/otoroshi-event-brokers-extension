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
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokersExtension, EventBrokersExtensionDatastores, EventBrokersExtensionState}
import play.api.libs.json._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

sealed trait EventBrokerConnectionKind {
  def name: String
  def json: JsValue = name.json
}

object EventBrokerConnectionKind {
  case object Kafka extends EventBrokerConnectionKind { def name: String = "kafka" }
  case object RabbitMQ extends EventBrokerConnectionKind { def name: String = "rabbitmq" }
  case object Pulsar extends EventBrokerConnectionKind { def name: String = "pulsar" }
  case object Redis extends EventBrokerConnectionKind { def name: String = "redis" }
  case object MQTT extends EventBrokerConnectionKind { def name: String = "mqtt" }
  def apply(str: String): Option[EventBrokerConnectionKind] = str.toLowerCase match {
    case "kafka" => Kafka.some
    case "rabbitmq" => RabbitMQ.some
    case "pulsar" => Pulsar.some
    case "redis" => Redis.some
    case "mqtt" => MQTT.some
    case _ => None
  }
}

// TODO: support kafka
// TODO: support rabbitmq
// TODO: support pulsar
// TODO: support mqtt
// TODO: support redis
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
  def hash: String = ???
  def client(ext: EventBrokersExtension)(implicit env: Env, ec: ExecutionContext): EventBrokerClient = {
    val client = EventBrokerConnection.cache.getOrElseUpdate(id, {
      kind match {
        case EventBrokerConnectionKind.Kafka => new KafkaBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.Pulsar => new PulsarBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.Redis => new RedisBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.MQTT => new MQTTBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.RabbitMQ => new RabbitMqBrokerClient(this, ext, env)
      }
    })
    if (client.hash != hash) {
      val newClient = kind match {
        case EventBrokerConnectionKind.Kafka => new KafkaBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.Pulsar => new PulsarBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.Redis => new RedisBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.MQTT => new MQTTBrokerClient(this, ext, env)
        case EventBrokerConnectionKind.RabbitMQ => new RabbitMqBrokerClient(this, ext, env)
      }
      EventBrokerConnection.cache.put(id, newClient)
      client.close()
      newClient
    } else {
      client
    }
  }
}

object EventBrokerConnection {
  val cache = new TrieMap[String, EventBrokerClient]()
  val format = new Format[EventBrokerConnection] {
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
        tmpl = (v, p) => {
          p.get("kind").map(_.toLowerCase()) match {
            case Some("kafka") => EventBrokerConnection(
              id = IdGenerator.namedId("provider", env),
              name = "Kafka connection",
              description = "A Kafka connection",
              metadata = Map.empty,
              tags = Seq.empty,
              location = EntityLocation.default,
              kind = EventBrokerConnectionKind.Kafka,
              settings = Json.obj(),
            ).json
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
  override def fmt: Format[EventBrokerConnection]      = EventBrokerConnection.format
  override def redisLike(implicit env: Env): RedisLike = redisCli
  override def key(id: String): String                 = s"${_env.storageRoot}:extensions:${extensionId.cleanup}:connections:$id"
  override def extractId(value: EventBrokerConnection): String    = value.id
}
