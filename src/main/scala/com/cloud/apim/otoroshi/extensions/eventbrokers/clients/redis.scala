package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.EventBrokerConnection
import com.cloud.apim.otoroshi.extensions.eventbrokers.{EventBrokerClient, EventWrapper}
import io.lettuce.core.resource.{ClientResources, DefaultClientResources}
import io.lettuce.core.{RedisClient, RedisURI}
import otoroshi.env.Env
import otoroshi.storage.drivers.lettuce.ByteStringRedisCodec
import otoroshi.utils.reactive.ReactiveStreamUtils
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json.{JsObject, JsValue, Json}
import reactor.core.publisher.Flux

import scala.concurrent.{ExecutionContext, Future}

class RedisBrokerClient(config: EventBrokerConnection, extension: EventBrokersExtension, env: Env) extends EventBrokerClient {

  println("new RedisBrokerClient")

  lazy val startTLS               = config.settings.select("start_tls").asOpt[Boolean].getOrElse(false)
  lazy val verifyPeers            = config.settings.select("verify_peers").asOpt[Boolean].getOrElse(true)
  lazy val nodesRaw               = config.settings.select("uris").asOpt[Seq[String]].toSeq.flatten.map { v =>
    val uri = RedisURI.create(v)
    uri.setStartTls(startTLS)
    uri.setVerifyPeer(verifyPeers)
    uri
  }
  lazy val resources = {
    val default = DefaultClientResources.builder().build()
    val computationThreadPoolSize = config.settings.select("computation_thread_pool_size").asOpt[Int].getOrElse(default.computationThreadPoolSize())
    val ioThreadPoolSize = config.settings.select("io_thread_pool_size").asOpt[Int].getOrElse(default.ioThreadPoolSize())
    ClientResources
      .builder()
      .computationThreadPoolSize(computationThreadPoolSize)
      .ioThreadPoolSize(ioThreadPoolSize)
      .build()
  }
  val client = RedisClient.create(resources, nodesRaw.head)
  val connection = client.connectPubSub(new ByteStringRedisCodec()).reactive()

  lazy val logger = play.api.Logger("RedisBrokerClient")

  def close()(implicit env: Env, ec: ExecutionContext): Future[Unit] = {
    client.close()
    ().vfuture
  }

  override def publish(event: ByteString, opts: JsObject)(implicit env: Env, ec: ExecutionContext): Future[Either[JsValue, Option[JsValue]]] = {
    val topic = opts.select("topic").asOpt[String].orElse(opts.select("channel").asOpt[String]).getOrElse(config.settings.select("channel").as[String])
    ReactiveStreamUtils.MonoUtils.toFuture(connection.publish(topic, event)).map { r =>
      Right(None)
    }
    .recover {
      case e: Throwable => Left(Json.obj("error" -> e.getMessage))
    }
  }

  override def consume(opts: JsObject)(implicit env: Env, ec: ExecutionContext): Source[EventWrapper, _] = {
    val topic = opts.select("topic").asOpt[String].orElse(opts.select("channel").asOpt[String]).getOrElse(config.settings.select("channel").as[String])
    connection.subscribe(topic)
    val f: Flux[EventWrapper] = connection.observeChannels().map { cm =>
      EventWrapper(None, cm.getMessage)
    }
    Source.fromPublisher(f)
  }
}