package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.{ConnectionManager, ConnectionManagerSettings, ConnectionTryout, EventBrokersWorkflows, ReplyListeners, WebhookLocks, WebhookManager, WebhookManagerSettings}
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities._
import otoroshi.env.Env
import otoroshi.models._
import otoroshi.next.extensions._
import otoroshi.utils.cache.types.UnboundedTrieMap
import otoroshi.utils.syntax.implicits._
import org.apache.pekko.stream.Materializer
import play.api.Logger
import play.api.libs.json.{JsArray, JsObject, Json}
import play.api.mvc.{RequestHeader, Results}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

class EventBrokersExtensionDatastores(env: Env, extensionId: AdminExtensionId) {
  val eventBrokerConnectionDatastore: EventBrokerConnectionDatastore = new KvEventBrokerConnectionDatastore(extensionId, env.datastores.redis, env)
  val eventBrokerWebhookDatastore: EventBrokerWebhookDatastore       = new KvEventBrokerWebhookDatastore(extensionId, env.datastores.redis, env)
}

class EventBrokersExtensionState {

  private val _connections = new UnboundedTrieMap[String, EventBrokerConnection]()
  def connection(id: String): Option[EventBrokerConnection] = _connections.get(id)
  def allConnections(): Seq[EventBrokerConnection]          = _connections.values.toSeq
  def updateConnections(values: Seq[EventBrokerConnection]): Unit = {
    _connections.addAll(values.map(v => (v.id, v))).remAll(_connections.keySet.toSeq.diff(values.map(_.id)))
  }

  private val _webhooks = new UnboundedTrieMap[String, EventBrokerWebhook]()
  def webhook(id: String): Option[EventBrokerWebhook] = _webhooks.get(id)
  def allWebhooks(): Seq[EventBrokerWebhook]          = _webhooks.values.toSeq
  def updateWebhooks(values: Seq[EventBrokerWebhook]): Unit = {
    _webhooks.addAll(values.map(v => (v.id, v))).remAll(_webhooks.keySet.toSeq.diff(values.map(_.id)))
  }
}

class EventBrokersExtension(val env: Env) extends AdminExtension {

  private lazy val datastores = new EventBrokersExtensionDatastores(env, id)

  lazy val states = new EventBrokersExtensionState()

  val logger = Logger("cloud-apim-event-brokers-extension")

  override def id: AdminExtensionId = AdminExtensionId("cloud-apim.extensions.EventBrokers")

  override def name: String = "Event Brokers Extension"

  override def description: Option[String] = "This extensions provides several plugins and connector deal with event brokers through Otoroshi".some

  override def enabled: Boolean = env.isDev || configuration.getOptional[Boolean]("enabled").getOrElse(false)

  /** the broker clients of this node */
  lazy val connections: ConnectionManager =
    new ConnectionManager(env, ConnectionManagerSettings.from(configuration), _.newClientInstance()(using env))

  /** the webhooks this node delivers: in a cluster, one node at a time each, decided in a shared redis */
  lazy val webhooks: WebhookManager = {
    val settings = WebhookManagerSettings.from(configuration)
    new WebhookManager(env, settings, connections, node => WebhookLocks(env, settings, node))
  }

  /** the replies this node waits for, for the request reply plugin */
  lazy val replies: ReplyListeners = new ReplyListeners(env, connections)

  override def start(): Unit = {
    logger.info("the 'Event Brokers Extension' is enabled !")
    connections.start()
    EventBrokersWorkflows.register()
  }

  override def stop(): Unit = {
    replies.stop()
    webhooks.stop()
    connections.stop()
  }

  // what this node sees: every node has its own clients, and the admin api answers for the node
  // serving it (the leader, in a cluster)
  override def adminApiRoutes(): Seq[AdminExtensionAdminApiRoute] = Seq(
    AdminExtensionAdminApiRoute(
      "GET",
      "/api/extensions/cloud-apim/event-brokers/connections/_status",
      false,
      (_, _, _, _) => {
        Results.Ok(Json.obj("node" -> nodeJson, "connections" -> JsArray(connections.statusesJson()))).vfuture
      }
    ),
    AdminExtensionAdminApiRoute(
      "POST",
      "/api/extensions/cloud-apim/event-brokers/connections/:id/_publish",
      true,
      (ctx, request, _, body) => {
        given ExecutionContext = env.otoroshiExecutionContext
        given Env              = env
        ConnectionTryout.publish(connections, ctx.named("id").getOrElse(""), request, body)
      }
    ),
    AdminExtensionAdminApiRoute(
      "GET",
      "/api/extensions/cloud-apim/event-brokers/connections/:id/_peek",
      false,
      (ctx, request, _, _) => {
        given ExecutionContext = env.otoroshiExecutionContext
        given Env              = env
        ConnectionTryout.peek(connections, ctx.named("id").getOrElse(""), request)
      }
    ),
    AdminExtensionAdminApiRoute(
      "GET",
      "/api/extensions/cloud-apim/event-brokers/webhooks/_status",
      false,
      (_, _, _, _) => {
        given ExecutionContext = env.otoroshiExecutionContext
        webhooks.statusesJson().map(all => Results.Ok(Json.obj("node" -> (nodeJson ++ webhooks.json), "webhooks" -> JsArray(all))))
      }
    ),
    AdminExtensionAdminApiRoute(
      "GET",
      "/api/extensions/cloud-apim/event-brokers/webhooks/:id/_status",
      false,
      (ctx, _, _, _) => {
        given ExecutionContext = env.otoroshiExecutionContext
        ctx.named("id").map(webhooks.statusJson).getOrElse(Future.successful(None)).map {
          case Some(status) => Results.Ok(status ++ Json.obj("node" -> (nodeJson ++ webhooks.json)))
          case None         => Results.NotFound(Json.obj("error" -> "not_found", "error_description" -> "no such webhook delivered by this node (unknown, or disabled)"))
        }
      }
    ),
    AdminExtensionAdminApiRoute(
      "GET",
      "/api/extensions/cloud-apim/event-brokers/connections/:id/_status",
      false,
      (ctx, _, _, _) => {
        ctx.named("id").flatMap(connections.statusJson) match {
          case Some(status) => Results.Ok(status ++ Json.obj("node" -> nodeJson)).vfuture
          case None         => Results.NotFound(Json.obj("error" -> "not_found", "error_description" -> "no such connection")).vfuture
        }
      }
    )
  )

  private def nodeJson: JsObject = Json.obj("cluster_mode" -> env.clusterConfig.mode.name)

  override def frontendExtensions(): Seq[AdminExtensionFrontendExtension] = Seq(
    AdminExtensionFrontendExtension(
      path = "/extensions/assets/cloud-apim/extensions/event-brokers/extension.js"
    )
  )

  def getResourceCode(path: String): String = {
    given ec: ExecutionContext = env.otoroshiExecutionContext
    given mat: Materializer    = env.otoroshiMaterializer
    env.environment.resourceAsStream(path)
      .map(stream => StreamConverters.fromInputStream(() => stream).runFold(ByteString.empty)(_++_).awaitf(10.seconds).utf8String)
      .getOrElse(s"'resource ${path} not found !'")
  }

  lazy val eventBrokerConnectionsPageCode = getResourceCode("cloudapim/extensions/event-brokers/EventBrokerConnectionsPage.js")
  lazy val eventBrokerWebhooksPageCode    = getResourceCode("cloudapim/extensions/event-brokers/EventBrokerWebhooksPage.js")
  lazy val imgCode = getResourceCode("cloudapim/extensions/event-brokers/icon.svg")

  override def assets(): Seq[AdminExtensionAssetRoute] = Seq(
    AdminExtensionAssetRoute(
      path = "/extensions/assets/cloud-apim/extensions/event-brokers/icon.svg",
      handle = (_: AdminExtensionRouterContext[AdminExtensionAssetRoute], _: RequestHeader) => {
        Results.Ok(imgCode).as("image/svg+xml").vfuture
      }
    ),
    AdminExtensionAssetRoute(
      path = "/extensions/assets/cloud-apim/extensions/event-brokers/extension.js",
      handle = (_: AdminExtensionRouterContext[AdminExtensionAssetRoute], _: RequestHeader) => {
        Results.Ok(
          s"""(function() {
             |  const extensionId = "${id.value}";
             |  Otoroshi.registerExtension(extensionId, false, (ctx) => {
             |
             |    const dependencies = ctx.dependencies;
             |
             |    const React     = dependencies.react;
             |    const _         = dependencies.lodash;
             |    const Component = React.Component;
             |    const uuid      = dependencies.uuid;
             |    const Table     = dependencies.Components.Inputs.Table;
             |    const SelectInput = dependencies.Components.Inputs.SelectInput;
             |    const BackOfficeServices = dependencies.BackOfficeServices;
             |
             |    ${eventBrokerConnectionsPageCode}
             |
             |    ${eventBrokerWebhooksPageCode}
             |
             |    return {
             |      id: extensionId,
             |      categories:[{
             |        title: 'Event Brokers',
             |        description: 'All the features provided the Cloud APIM Event Brokers extension',
             |        features: [
             |          {
             |            title: 'Event broker connections',
             |            description: 'All your event broker connections',
             |            absoluteImg: '/extensions/assets/cloud-apim/extensions/event-brokers/icon.svg',
             |            link: '/extensions/cloud-apim/event-brokers/connections',
             |            display: () => true,
             |            icon: () => 'fa-atom',
             |          },
             |          {
             |            title: 'Event broker webhooks',
             |            description: 'Events pushed to http endpoints',
             |            absoluteImg: '/extensions/assets/cloud-apim/extensions/event-brokers/icon.svg',
             |            link: '/extensions/cloud-apim/event-brokers/webhooks',
             |            display: () => true,
             |            icon: () => 'fa-paper-plane',
             |          }
             |        ]
             |      }],
             |      features: [
             |        {
             |          title: 'Event broker connections',
             |          description: 'All your Event broker connections',
             |          absoluteImg: '/extensions/assets/cloud-apim/extensions/event-brokers/icon.svg',
             |          link: '/extensions/cloud-apim/event-brokers/connections',
             |          display: () => true,
             |          icon: () => 'fa-atom',
             |        },
             |        {
             |          title: 'Event broker webhooks',
             |          description: 'Events pushed to http endpoints',
             |          absoluteImg: '/extensions/assets/cloud-apim/extensions/event-brokers/icon.svg',
             |          link: '/extensions/cloud-apim/event-brokers/webhooks',
             |          display: () => true,
             |          icon: () => 'fa-paper-plane',
             |        }
             |      ],
             |      sidebarItems: [
             |        {
             |          title: 'Event broker connections',
             |          text: 'All your Event broker connections',
             |          path: 'extensions/cloud-apim/event-brokers/connections',
             |          icon: 'atom'
             |        },
             |        {
             |          title: 'Event broker webhooks',
             |          text: 'Events pushed to http endpoints',
             |          path: 'extensions/cloud-apim/event-brokers/webhooks',
             |          icon: 'paper-plane'
             |        }
             |      ],
             |      searchItems: [
             |        {
             |          action: () => {
             |            window.location.href = `/bo/dashboard/extensions/cloud-apim/event-brokers/connections`
             |          },
             |          env: React.createElement('span', { className: "fas fa-atom" }, null),
             |          label: 'Event brokers connections',
             |          value: 'eventbrokerconnections',
             |        },
             |        {
             |          action: () => {
             |            window.location.href = `/bo/dashboard/extensions/cloud-apim/event-brokers/webhooks`
             |          },
             |          env: React.createElement('span', { className: "fas fa-paper-plane" }, null),
             |          label: 'Event brokers webhooks',
             |          value: 'eventbrokerwebhooks',
             |        }
             |      ],
             |      routes: [
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/webhooks/:taction/:titem',
             |          component: (props) => {
             |            return React.createElement(EventBrokerWebhooksPage, props, null)
             |          }
             |        },
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/webhooks/:taction',
             |          component: (props) => {
             |            return React.createElement(EventBrokerWebhooksPage, props, null)
             |          }
             |        },
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/webhooks',
             |          component: (props) => {
             |            return React.createElement(EventBrokerWebhooksPage, props, null)
             |          }
             |        },
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/connections/:taction/:titem',
             |          component: (props) => {
             |            return React.createElement(EventBrokerConnectionsPage, props, null)
             |          }
             |        },
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/connections/:taction',
             |          component: (props) => {
             |            return React.createElement(EventBrokerConnectionsPage, props, null)
             |          }
             |        },
             |        {
             |          path: '/extensions/cloud-apim/event-brokers/connections',
             |          component: (props) => {
             |            return React.createElement(EventBrokerConnectionsPage, props, null)
             |          }
             |        }
             |      ]
             |    }
             |  });
             |})();
             |""".stripMargin).as("application/javascript").vfuture
      }
    )
  )

  override def syncStates(): Future[Unit] = {
    given ec: ExecutionContext = env.otoroshiExecutionContext
    given ev: Env              = env
    for {
      connections <- datastores.eventBrokerConnectionDatastore.findAllAndFillSecrets()
      webhooks    <- datastores.eventBrokerWebhookDatastore.findAllAndFillSecrets()
    } yield {
      states.updateConnections(connections)
      states.updateWebhooks(webhooks)
      this.connections.sync(connections)
      this.webhooks.sync(webhooks)
      ()
    }
  }

  override def entities(): Seq[AdminExtensionEntity[EntityLocationSupport]] = {
    Seq(
      AdminExtensionEntity(EventBrokerConnection.resource(env, datastores, states)),
      AdminExtensionEntity(EventBrokerWebhook.resource(env, datastores, states)),
    )
  }
}
