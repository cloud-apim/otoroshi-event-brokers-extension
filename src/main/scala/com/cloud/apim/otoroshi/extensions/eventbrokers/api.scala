package com.cloud.apim.otoroshi.extensions.eventbrokers

import akka.stream.scaladsl.Source
import akka.util.ByteString
import otoroshi.env.Env
import play.api.libs.json.{JsObject, JsValue}
import otoroshi.utils.syntax.implicits._

import scala.concurrent.{ExecutionContext, Future}

case class EventWrapper(id: Option[String], data: ByteString) {
  def serialize: ByteString = id match {
    case Some(id) => s"id: ${id}\ndata: ${data.utf8String}\n\n".byteString
    case None => s"data: ${data.utf8String}\n\n".byteString
  }
}

trait EventBrokerClient {
  def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]]
  def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _]
  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit]
}
