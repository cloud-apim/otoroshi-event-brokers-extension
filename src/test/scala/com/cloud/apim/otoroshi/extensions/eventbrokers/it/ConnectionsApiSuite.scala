package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{BrokerSettings, EventBrokerConnectionKind}
import play.api.libs.json.*
import play.api.libs.ws.DefaultBodyWritables.writeableOf_String

import java.util.UUID
import scala.concurrent.duration.*

/** what the admin api accepts as connections, and what becomes of those it would not have accepted (no docker) */
class ConnectionsApiSuite extends munit.FunSuite {

  override val munitTimeout: Duration = 2.minutes

  import Gateway.{await, eventually}

  def body(kind: String, settings: JsObject, id: String = s"connection_${UUID.randomUUID()}"): JsObject = Json.obj(
    "id"          -> id,
    "name"        -> s"$kind connection",
    "description" -> "",
    "tags"        -> Json.arr(),
    "metadata"    -> Json.obj(),
    "kind"        -> kind,
    "settings"    -> settings
  )

  def post(json: JsValue) = await(Gateway.admin(Gateway.connectionsPath).post(Json.stringify(json)))
  def put(id: String, json: JsValue) = await(Gateway.admin(s"${Gateway.connectionsPath}/$id").put(Json.stringify(json)))
  def get(id: String) = await(Gateway.admin(s"${Gateway.connectionsPath}/$id").get())

  def description(res: play.api.libs.ws.WSResponse): String = (res.json \ "error_description").asOpt[JsValue].map {
    case JsString(s)  => s
    case JsArray(arr) => arr.map(_.as[String]).mkString("; ")
    case other        => other.toString
  }.getOrElse(res.body)

  test("settings that do not fit the kind are refused, with every field at fault") {
    val res = post(body("kafka", Json.obj("servers" -> Json.arr("k:9092"), "topic" -> "t")))
    assertEquals(res.status, 400, res.body)
    val text = description(res)
    Seq("settings.bootstrap_servers: required", "settings.servers: unknown field", "settings.topic: unknown field").foreach { e =>
      assert(text.contains(e), s"'$e' missing in $text")
    }
  }

  test("an unknown kind is refused") {
    val res = post(body("nats", Json.obj()))
    assertEquals(res.status, 400, res.body)
    assert(description(res).contains("kind: must be one of"), res.body)
  }

  test("the template of every kind is a valid connection") {
    EventBrokerConnectionKind.values.foreach { kind =>
      val template = await(Gateway.admin(s"${Gateway.connectionsPath}/_template?kind=${kind.name}").get())
      assertEquals(template.status, 200, template.body)
      assertEquals((template.json \ "kind").as[String], kind.name)
      assertEquals((template.json \ "settings").as[JsObject], BrokerSettings.template(kind).json)
      val created  = post(template.json)
      assert(created.status < 300, s"${kind.name}: ${created.status} ${created.body}")
      Gateway.deleteConnection((template.json \ "id").as[String])
    }
  }

  test("an update with wrong settings is refused, and leaves the connection as it was") {
    val id      = s"connection_${UUID.randomUUID()}"
    val created = post(body("redis", Json.obj("uri" -> "redis://127.0.0.1:1"), id))
    assert(created.status < 300, created.body)
    val res     = put(id, body("redis", Json.obj("uri" -> "http://nope"), id))
    assertEquals(res.status, 400, res.body)
    assertEquals((get(id).json \ "settings" \ "uri").as[String], "redis://127.0.0.1:1")
    Gateway.deleteConnection(id)
  }

  // written by an older version, or by hand: the api must keep showing it so that it can be fixed
  test("a stored connection with wrong settings stays visible, and is unavailable without trying again") {
    val id  = s"connection_${UUID.randomUUID()}"
    val env = Gateway.instance.env
    val key = s"${env.storageRoot}:extensions:cloud-apim_extensions_eventbrokers:connections:$id"
    await(env.datastores.redis.set(key, Json.stringify(body("kafka", Json.obj("servers" -> Json.arr("k:9092")), id))))
    eventually("visible in the api")(get(id).status == 200)
    eventually("unavailable")(Gateway.connectionState(id).map(_.name).contains("unavailable"))
    val status = Gateway.connectionStatus(id).json
    assert((status \ "error").as[String].startsWith("invalid settings: bootstrap_servers: required"), status.toString)
    assertEquals((status \ "next_attempt_at").asOpt[Long], None)
    Thread.sleep(1500L)
    assertEquals((Gateway.connectionStatus(id).json \ "failures").as[Int], 1)
    // fixed through the api, it connects (or tries to)
    val fixed = put(id, body("kafka", Json.obj("bootstrap_servers" -> Json.arr("127.0.0.1:1")), id))
    assert(fixed.status < 300, fixed.body)
    eventually("trying again")((Gateway.connectionStatus(id).json \ "next_attempt_at").asOpt[Long].isDefined)
    Gateway.deleteConnection(id)
  }
}
