# Cloud APIM - Event Brokers for Otoroshi

An [Otoroshi](https://www.otoroshi.io/) extension that **puts event brokers behind your routes**:
publish events with a plain HTTP call, and consume them as a stream of JSON lines, as server-sent
events, or over a websocket. The brokers stay private, Otoroshi handles who may reach them.

> **Status:** the extension is being reworked into a production-grade product (per-broker features,
> delivery guarantees, observability, documentation). The test suite pins every known defect of the
> current implementation with a `.fail` test (see [Running the tests](#running-the-tests)).

## Features

### Brokers

| Kind       | Broker                          | Client                          |
|------------|---------------------------------|---------------------------------|
| `kafka`    | Apache Kafka                    | pekko-connectors-kafka (otoroshi) |
| `rabbitmq` | RabbitMQ (AMQP 0.9.1)           | amqp-client 5                   |
| `pulsar`   | Apache Pulsar                   | pulsar4s (otoroshi)             |
| `redis`    | Redis pub/sub                   | lettuce (otoroshi)              |
| `mqtt`     | any MQTT 3.1.1 broker           | eclipse paho                    |

### Plugins

| Plugin                                      | Identifier                                                                                   | Does                                                  |
|---------------------------------------------|----------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Cloud APIM - HTTP events ingester           | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsIngester`        | publishes the request body (one event, a batch, or newline-delimited) |
| Cloud APIM - HTTP events consumer           | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsConsumer`        | streams events as newline-delimited JSON              |
| Cloud APIM - SSE events consumer            | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.SseEventConsumer`          | streams events as server-sent events                  |
| Cloud APIM - Websocket Ingester/Consumer    | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.IngesterConsumerWebsocket` | publishes incoming frames, streams events back        |

Every plugin points to an **event broker connection**. Per-call options (`topic`, `queue`, `channel`,
`group_id`, `auto_offset_reset`, `take`, `drop`, `duration`, ...) are read from query parameters and
headers prefixed with `x-ca-event-broker-opts-`.

### Connections

Every Otoroshi node keeps one client per connection, **connected in the background** as soon as the
connection exists: no request ever waits for a broker to be reached.

- A connection that cannot connect is **unavailable** and tries again, waiting twice as long each time
  (1s up to 30s). Its routes answer `503` with a `Retry-After` at once.
- A connected client is **checked** against its broker every 10s. A broker that stops answering makes
  the connection unavailable until it answers again; after 3 failed checks in a row the client is
  closed and built again.
- **New settings** get a client of their own, which takes over once connected: the previous settings
  serve the requests until then, and keep doing so if the new ones never connect (the status says
  why). The streams still open on the previous client may go on for 30s before it is closed.
- A deleted connection's client is closed the same way.

The state of the connections of a node is in the backoffice list, and in the admin API:

```bash
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/_status -u admin-api-apikey-id:admin-api-apikey-secret
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/<id>/_status -u ...
```

```json
{
  "id": "connection_...", "name": "orders", "kind": "kafka",
  "state": "connected", "since": 1792663171364, "error": null, "failures": 0,
  "last_check_at": 1792663181364, "next_attempt_at": null,
  "pending": null
}
```

`state` is `connecting`, `connected`, `unavailable` or `closed`. `pending` is the status of new settings
that are not connected yet. In a cluster each node has its own clients: the admin API answers for the
node serving it.

### Analytics events and metrics

| Event                                | `@type` / `alert`                                | Sent when                                   |
|--------------------------------------|--------------------------------------------------|---------------------------------------------|
| `EventBrokerConnectionEvent`         | `EventBrokerConnectionEvent`                     | a connection changes state on a node        |
| `EventBrokerConnectionUnavailableAlert` | `AlertEvent` / `EventBrokerConnectionUnavailable` | a connection cannot connect, or loses its broker |

Both carry the `connection` (`id`, `name`, `kind`), the `previous_state` and the `status` above. One alert
per outage, not one per attempt.

Metrics, when Otoroshi's metrics are enabled: `cloud-apim.event-brokers.connections.<state>` (how many
connections of the node are in each state) and `cloud-apim.event-brokers.connections.transitions.<state>`.

## Requirements

- Otoroshi **18.0.0** or later
- Java **17** or later

## Installation

1. Download `otoroshi-extension-event-brokers_3-<version>.jar` and its `.sha256` from the
   [releases page](https://github.com/cloud-apim/otoroshi-extension-event-brokers/releases/latest).
2. Start Otoroshi with the jar on its classpath:

   ```bash
   java -cp "./otoroshi-extension-event-brokers.jar:./otoroshi.jar" play.core.server.ProdServerStart
   ```

   or mount it into the plugins directory of the Otoroshi Docker image (`OTOROSHI_PLUGINS_DIR_PATH`).

The extension is enabled as soon as its jar is loaded. To turn it off:

```hocon
otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers.enabled = false
# or CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_ENABLED=false
```

The lifecycle of the connections can be tuned in the same block (defaults shown, each one also
readable from an environment variable, see [`reference.conf`](src/main/resources/reference.conf)):

```hocon
otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers.connections {
  connect-timeout = 10s
  retry.min-backoff = 1s
  retry.max-backoff = 30s
  healthcheck.interval = 10s
  healthcheck.timeout = 5s
  healthcheck.max-failures = 3
  retire-grace-period = 30s
  close-timeout = 10s
}
```

## Getting started

1. Open **Event broker connections** in the Otoroshi backoffice and create a connection to your broker.
2. Create a route and add one of the plugins above, pointing to that connection.
3. Publish:

   ```bash
   curl -X POST 'http://events.oto.tools:8080/?x-ca-event-broker-opts-topic=orders' \
     -H 'Content-Type: application/json' -d '{"id": 1}'
   ```

4. Consume, on a route with the HTTP or SSE consumer:

   ```bash
   curl -N 'http://events-stream.oto.tools:8080/?x-ca-event-broker-opts-topic=orders'
   ```

## Entities

| Entity                  | Kind                                                          | Admin API                                                                    |
|-------------------------|---------------------------------------------------------------|------------------------------------------------------------------------------|
| Event broker connection | `event-brokers.extensions.cloud-apim.com/EventBrokerConnection` | `/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections` |

## Running the tests

| Suites                                   | What they run against                                        | Needs   |
|------------------------------------------|--------------------------------------------------------------|---------|
| `EventWrapperSuite`, `PluginConfigSuite`, `ConnectionManagerSettingsSuite` | the code alone                 | nothing |
| `it.ConnectionManagerSuite`              | the connection lifecycle over fake brokers, on the env of an Otoroshi in the test jvm | nothing |
| `it.ConnectionsSuite`                    | the connection lifecycle against a real Redis: not there yet, paused, back, settings changed, deleted | docker  |
| `it.KafkaSuite`, `it.RabbitMQSuite`, `it.PulsarSuite`, `it.RedisSuite`, `it.MqttSuite` | an Otoroshi in the test jvm, with the extension loaded, and a real broker in a container | docker  |

The broker suites start their containers with [testcontainers](https://testcontainers.com/) and skip
themselves when no docker daemon is reachable. Each test publishes and consumes through real
Otoroshi routes.

Tests named `.fail` pin a known defect: they pass while the defect is there and start failing the day
it is fixed, which is when the marker goes.

```bash
sbt test                      # everything
sbt "testOnly *KafkaSuite"    # one broker
```

## Development

```bash
docker compose up -d          # every broker on its default port (kafka on 9093, kafka-ui on 8000)
sbt test                      # see above
sbt assembly                  # target/scala-3.8.4/otoroshi-extension-event-brokers-assembly_3-dev.jar
./rebuild.sh                  # assembly, linked into a local otoroshi checkout (../otoroshi/otoroshi/lib), reloaded
```

The jar is an assembly: the RabbitMQ and MQTT clients are not on Otoroshi's classpath, the Kafka,
Pulsar and Redis ones are.

CI runs every suite on each push and pull request. Releases are made by hand:
**Actions → release → Run workflow**, with the version. The workflow tests, builds the jar, tags the
commit and publishes the release with the jar and its checksum.

## License

Apache 2.0
