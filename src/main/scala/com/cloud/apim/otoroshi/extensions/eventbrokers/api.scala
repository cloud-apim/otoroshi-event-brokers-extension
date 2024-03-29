package com.cloud.apim.otoroshi.extensions.eventbrokers

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import otoroshi.env.Env
import play.api.libs.json.{JsNull, JsObject, JsValue, Json}
import otoroshi.utils.syntax.implicits._

import scala.concurrent.{ExecutionContext, Future}

object EventWrapper {
  def fromJson(json: JsValue): EventWrapper = {
    val keyContentType = json.select("key").select("type").asOpt[String].getOrElse("binary")
    val dataContentType = json.select("value").select("type").asOpt[String].getOrElse("binary")
    val key: Option[ByteString] = keyContentType.toLowerCase() match {
      case "text" => json.select("key").select("data").asOpt[String].map(_.byteString)
      case "text/plain" => json.select("key").select("data").asOpt[String].map(_.byteString)
      case "json" => json.select("key").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "application/json" => json.select("key").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "binary" => json.select("key").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case "application/octet-stream" => json.select("key").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case _ => None
    }
    val data: Option[ByteString] = dataContentType.toLowerCase() match {
      case "text" =>                     json.select("value").select("data").asOpt[String].map(_.byteString)
      case "text/plain" =>               json.select("value").select("data").asOpt[String].map(_.byteString)
      case "json" =>                     json.select("value").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "application/json" =>         json.select("value").select("data").asOpt[JsValue].map(_.stringify.byteString)
      case "binary" =>                   json.select("value").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case "application/octet-stream" => json.select("value").select("data").asOpt[String].map(_.byteString.decodeBase64)
      case _ => None
    }
    EventWrapper(
      key = key,
      data = data.getOrElse(ByteString.empty),
      keyContentType = keyContentType,
      dataContentType = dataContentType,
      headers = json.select("headers").asOpt[Map[String, String]].getOrElse(Map.empty),
      vendorMetadata = json.select("vendor_metadata").asOpt[Map[String, String]].getOrElse(Map.empty),
    )
  }
}

case class EventWrapper(
  key: Option[ByteString],
  data: ByteString,
  keyContentType: String,
  dataContentType: String,
  headers: Map[String, String],
  vendorMetadata: Map[String, String]
) {
  def wsMessage(enriched: Boolean): play.api.http.websocket.Message = {
    if (enriched) {
      play.api.http.websocket.TextMessage(json.stringify)
    } else {
      play.api.http.websocket.BinaryMessage(data)
    }
  }
  def sseSerialize(enriched: Boolean): ByteString = {
    if (enriched) {
      key match {
        case Some(id) => {
          val key: String = keyContentType.toLowerCase() match {
            case "text" => id.utf8String
            case "text/plain" => id.utf8String
            case "json" => id.utf8String
            case "application/json" => id.utf8String
            case _ => id.encodeBase64.utf8String
          }
          s"id: ${key}\ndata: ${json.stringify}\n\n".byteString
        } // TODO: support event: kind
        case None => s"data: ${json.stringify}\n\n".byteString
      }
    } else {
      key match {
        case Some(id) => {
          val key: String = keyContentType.toLowerCase() match {
            case "text" => id.utf8String
            case "text/plain" => id.utf8String
            case "json" => id.utf8String
            case "application/json" => id.utf8String
            case _ => id.encodeBase64.utf8String
          }
          s"id: ${key}\ndata: ${data.utf8String}\n\n".byteString
        } // TODO: support event: kind
        case None => s"data: ${data.utf8String}\n\n".byteString
      }
    }
  }
  def httpSerialize(enriched: Boolean): ByteString = {
    if (enriched) {
      json.stringify.byteString
    } else {
      data
    }
  }
  def jsonKey: JsValue = {
    key match {
      case None => JsNull
      case Some(key) => {
        val data = keyContentType.toLowerCase() match {
          case "text" => key.utf8String.json
          case "text/plain" => key.utf8String.json
          case "json" => key.utf8String.parseJson
          case "application/json" => key.utf8String.parseJson
          case "binary" => key.encodeBase64.utf8String.json
          case "application/octet-stream" => key.encodeBase64.utf8String.json
          case _ => JsNull
        }
        Json.obj(
          "type" -> keyContentType,
          "data" -> data
        )
      }
    }
  }
  def jsonValue: JsValue = {
    val finaldata = dataContentType.toLowerCase() match {
      case "text" => data.utf8String.json
      case "text/plain" => data.utf8String.json
      case "json" => data.utf8String.parseJson
      case "application/json" => data.utf8String.parseJson
      case "binary" => data.encodeBase64.utf8String.json
      case "application/octet-stream" => data.encodeBase64.utf8String.json
      case _ => JsNull
    }
    Json.obj(
      "type" -> dataContentType,
      "data" -> finaldata
    )
  }
  def json: JsValue = Json.obj(
    "key" -> jsonKey,
    "value" -> jsonValue,
    "headers" -> headers,
    "timestamp" -> headers.get("timestamp").orElse(vendorMetadata.get("timestamp")).map(_.json).getOrElse(JsNull).asValue,
    "vendor_metadata" -> vendorMetadata,
  )
}

trait EventBrokerClient {
  def publish(event: EventWrapper, opts: JsObject, contentType: String)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]]
  def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _]
  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit]
  def hash: String = config.hash
  def config: EventBrokerConnection
}
