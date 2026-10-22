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
| `EventWrapperSuite`, `PluginConfigSuite` | the code alone                                               | nothing |
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
