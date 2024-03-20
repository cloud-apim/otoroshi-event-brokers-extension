package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins

import akka.stream.Materializer
import akka.util.ByteString
import otoroshi.env.Env
import otoroshi.next.plugins.api._
import otoroshi.next.proxy.NgProxyEngineError
import otoroshi.utils.syntax.implicits._
import otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.EventBrokersExtension
import play.api.libs.json._
import play.api.mvc.Results

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class HttpEventsIngesterConfig(ref: String) extends NgPluginConfig {
  def json: JsValue = HttpEventsIngesterConfig.format.writes(this)
}

object HttpEventsIngesterConfig {
  val default = HttpEventsIngesterConfig("")
  val format = new Format[HttpEventsIngesterConfig] {
    override def reads(json: JsValue): JsResult[HttpEventsIngesterConfig] = Try {
      HttpEventsIngesterConfig(
        ref = json.select("ref").asOpt[String].getOrElse(""),
      )
    } match {
      case Failure(e) => JsError(e.getMessage)
      case Success(e) => JsSuccess(e)
    }
    override def writes(o: HttpEventsIngesterConfig): JsValue = Json.obj(
      "ref" -> o.ref,
    )
  }
  val configFlow: Seq[String] = Seq("ref")
  val configSchema: Option[JsObject] = Some(Json.obj(
    "ref" -> Json.obj(
      "type" -> "select",
      "label" -> s"Event Broker Connection",
      "props" -> Json.obj(
        "optionsFrom" -> s"/bo/api/proxy/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections",
        "optionsTransformer" -> Json.obj(
          "label" -> "name",
          "value" -> "id",
        ),
      ),
    )
  ))
}

class HttpEventsIngester extends NgBackendCall {

  override def name: String = "Cloud APIM - HTTP events ingester"
  override def description: Option[String] = "Can ingest events and send them to an event broker".some
  override def core: Boolean = false
  override def visibility: NgPluginVisibility = NgPluginVisibility.NgUserLand
  override def categories: Seq[NgPluginCategory] = Seq(NgPluginCategory.Custom("Cloud APIM"), NgPluginCategory.Custom("Event Brokerss"))

  override def steps: Seq[NgStep] = Seq(NgStep.CallBackend)
  override def defaultConfigObject: Option[NgPluginConfig] = Some(HttpEventsIngesterConfig.default)
  override def noJsForm: Boolean = true
  override def configFlow: Seq[String] = HttpEventsIngesterConfig.configFlow
  override def configSchema: Option[JsObject] = HttpEventsIngesterConfig.configSchema

  override def useDelegates: Boolean = false

  override def start(env: Env): Future[Unit] = {
    env.adminExtensions.extension[EventBrokersExtension].foreach { ext =>
      ext.logger.info("the 'HTTP POST events ingester' plugin is available !")
    }
    ().vfuture
  }

  override def callBackend(ctx: NgbBackendCallContext, delegates: () => Future[Either[NgProxyEngineError, BackendCallResponse]])(implicit env: Env, ec: ExecutionContext, mat: Materializer): Future[Either[NgProxyEngineError, BackendCallResponse]] = {
    val config = ctx.cachedConfig(internalName)(HttpEventsIngesterConfig.format).getOrElse(HttpEventsIngesterConfig.default)
    env.adminExtensions.extension[EventBrokersExtension] match {
      case None => Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.InternalServerError(Json.obj("error" -> "internal_server_error", "error_description" -> "extension not available")))).vfuture
      case Some(extension) => extension.states.connection(config.ref) match {
        case None => Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.InternalServerError(Json.obj("error" -> "internal_server_error", "error_description" -> "connection not found")))).vfuture
        case Some(connection) => {
          if (ctx.request.hasBody) {
            ctx.request.body.runFold(ByteString.empty)(_ ++ _).flatMap { bodyRaw =>
              val client = connection.client(extension)
              client.publish(bodyRaw, JsObject(ctx.request.uri.query().toMap.mapValues(_.json))).map {
                case Left(err) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.InternalServerError(Json.obj("error" -> "publish_error", "error_details" -> err))), None))
                case Right(Some(details)) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Created(Json.obj("published" -> true, "details" -> details))), None))
                case Right(None) => Right(BackendCallResponse(NgPluginHttpResponse.fromResult(Results.Created(Json.obj("published" -> true))), None))
              }
            }
          } else {
            Left(NgProxyEngineError.NgResultProxyEngineError.apply(Results.BadRequest(Json.obj("error" -> "bad_request", "error_description" -> "no body provided")))).vfuture
          }
        }
      }
    }
  }
}
