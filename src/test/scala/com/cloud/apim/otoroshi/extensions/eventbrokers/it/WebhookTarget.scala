package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.util.ByteString

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** what a webhook endpoint got */
final case class Delivery(method: String, path: String, headers: Map[String, String], contentType: String, body: ByteString) {
  def header(name: String): Option[String] = headers.collectFirst { case (k, v) if k.equalsIgnoreCase(name) => v }
}

/**
 * An http endpoint in the test jvm, standing for the service a webhook calls: it keeps what it gets,
 * and answers what `answer` says (from the number of the request, from 1).
 */
final class WebhookTarget(answer: Int => StatusCode = _ => StatusCodes.OK) {

  import Gateway.given

  val received = new LinkedBlockingQueue[Delivery]()
  private val count = new AtomicInteger(0)

  private val binding = Gateway.await(
    Http()
      .newServerAt("127.0.0.1", 0)
      .bind { request =>
        request.entity.toStrict(10.seconds).map { entity =>
          val status = answer(count.incrementAndGet())
          received.add(
            Delivery(
              request.method.value,
              request.uri.path.toString,
              request.headers.map(h => h.name -> h.value).toMap,
              entity.contentType.toString,
              entity.data
            )
          )
          HttpResponse(status)
        }
      }
  )

  val url: String = s"http://127.0.0.1:${binding.localAddress.getPort}/hook"

  def requests: Int = count.get()

  /** the next delivery, if one comes within `within` */
  def next(within: FiniteDuration = 20.seconds): Option[Delivery] = Option(received.poll(within.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS))

  def all: Seq[Delivery] = received.asScala.toSeq

  def stop(): Unit = { Gateway.await(binding.unbind()); () }
}
