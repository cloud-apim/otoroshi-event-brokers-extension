package otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers

import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.pekko.util.ByteString
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities._
import otoroshi.env.Env
import otoroshi.models._
import otoroshi.next.extensions._
import otoroshi.utils.cache.types.UnboundedTrieMap
import otoroshi.utils.syntax.implicits._
import org.apache.pekko.stream.Materializer
import play.api.Logger
import play.api.mvc.{RequestHeader, Results}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

class EventBrokersExtensionDatastores(env: Env, extensionId: AdminExtensionId) {
  val eventBrokerConnectionDatastore: EventBrokerConnectionDatastore = new KvEventBrokerConnectionDatastore(extensionId, env.datastores.redis, env)
}

class EventBrokersExtensionState {

  private val _connections = new UnboundedTrieMap[String, EventBrokerConnection]()
  def connection(id: String): Option[EventBrokerConnection] = _connections.get(id)
  def allConnections(): Seq[EventBrokerConnection]          = _connections.values.toSeq
  def updateConnections(values: Seq[EventBrokerConnection]): Unit = {
    _connections.addAll(values.map(v => (v.id, v))).remAll(_connections.keySet.toSeq.diff(values.map(_.id)))
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

  override def start(): Unit = {
    logger.info("the 'Event Brokers Extension' is enabled !")
  }

  override def stop(): Unit = {
  }

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
             |        }
             |      ],
             |      sidebarItems: [
             |        {
             |          title: 'Event broker connections',
             |          text: 'All your Event broker connections',
             |          path: 'extensions/cloud-apim/event-brokers/connections',
             |          icon: 'atom'
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
             |        }
             |      ],
             |      routes: [
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
    } yield {
      states.updateConnections(connections)
      ()
    }
  }

  override def entities(): Seq[AdminExtensionEntity[EntityLocationSupport]] = {
    Seq(
      AdminExtensionEntity(EventBrokerConnection.resource(env, datastores, states)),
    )
  }
}
