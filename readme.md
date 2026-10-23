# Cloud APIM - Event Brokers for Otoroshi

An [Otoroshi](https://www.otoroshi.io/) extension that **puts event brokers behind your routes**:
publish events with a plain HTTP call, consume them as newline-delimited JSON, as server-sent events
or over a websocket, push them to webhooks. The brokers stay private, Otoroshi decides who may reach
them.

**Documentation: <https://cloud-apim.github.io/otoroshi-event-brokers-extension/>**

| `broker`   | Broker                                              | Client                              |
|------------|-----------------------------------------------------|-------------------------------------|
| `kafka`    | Apache Kafka                                        | pekko-connectors-kafka (otoroshi)   |
| `rabbitmq` | RabbitMQ (AMQP 0.9.1), classic, quorum and stream queues | amqp-client 5                  |
| `pulsar`   | Apache Pulsar                                       | Pulsar client (otoroshi)            |
| `redis`    | Redis pub/sub, or Redis streams                     | Lettuce (otoroshi)                  |
| `mqtt`     | any MQTT 5 or 3.1.1 broker, over TCP or websockets  | HiveMQ MQTT client                  |
| `artemis`  | ActiveMQ Artemis, on its core protocol              | Artemis core client (otoroshi)      |

## What it does

- **Connections** kept by every node in the background: checked, rebuilt, switched to new settings
  without a dropped request; TLS with the certificates of the Otoroshi store, SASL, tokens, vaults.
- **Routes**: an HTTP ingester (one result per event, once the broker took it), HTTP, server-sent
  events and websocket consumers (groups, at-least-once delivery, replay, resume where a client
  stopped), request reply. The route decides what its callers may ask.
- **CloudEvents** everywhere, with the protocol binding of each broker.
- **Each broker at its best**: Kafka keys, offsets and producer tuning; RabbitMQ confirms, topologies
  and streams; Pulsar subscriptions; Redis streams groups and claims; MQTT 5 shared subscriptions;
  Artemis routing types, selectors, message groups, duplicate detection.
- **Webhooks**: events pushed to HTTP endpoints, signed, retried, dead-lettered, delivered by one node
  of a cluster at a time.
- **Integrations**: a data exporter for Otoroshi's own events, workflow functions.
- **Observability**: connection events and alerts, traffic counters, the lag of the groups, metrics.

## Install

Otoroshi 18 and Java 17 or later. Download `otoroshi-event-brokers-extension_3-<version>.jar` from the
[releases page](https://github.com/cloud-apim/otoroshi-event-brokers-extension/releases/latest) and put
it on Otoroshi's classpath:

```bash
java -cp "./otoroshi-event-brokers-extension_3-<version>.jar:./otoroshi.jar" play.core.server.ProdServerStart
```

Then follow the [quickstart](https://cloud-apim.github.io/otoroshi-event-brokers-extension/docs/quickstart).

## Running the tests

| Suites                                   | What they run against                                        | Needs   |
|------------------------------------------|--------------------------------------------------------------|---------|
| `CloudEventSuite`, `PluginConfigSuite`, `ConnectionManagerSettingsSuite`, `SettingsSuite`, `KafkaPropertiesSuite` | the code alone | nothing |
| `it.ConnectionManagerSuite`              | the connection lifecycle over fake brokers, on the env of an Otoroshi in the test jvm | nothing |
| `it.ConnectionsApiSuite`                 | what the admin api accepts as connections, on an Otoroshi in the test jvm | nothing |
| `it.PolicySuite`                         | what a caller may ask under the configuration of its plugin, against Kafka | docker  |
| `it.ConnectionsSuite`                    | the connection lifecycle against a real Redis: not there yet, paused, back, settings changed, deleted | docker  |
| `it.KafkaSuite`, `it.RabbitMQSuite`, `it.PulsarSuite`, `it.RedisSuite`, `it.RedisStreamsSuite`, `it.MqttSuite`, `it.ArtemisSuite` | an Otoroshi in the test jvm, with the extension loaded, and a real broker in a container | docker  |
| `it.TlsSuite`                            | mutual TLS against every broker, with a test CA and certificates created in the Otoroshi store | docker  |
| `it.AuthSuite`                           | authentication against every broker: Kafka SASL SCRAM and PLAIN, Pulsar tokens, Redis ACL users, MQTT and RabbitMQ passwords | docker  |
| `it.WebhookSuite`                        | webhooks against an HTTP endpoint in the test jvm: modes, signature, retries, dead letters, alerts, two nodes sharing their locks in Redis | docker  |
| `it.TryoutSuite`, `it.IntegrationsSuite` | the publish and peek admin routes, the data exporter, the workflow functions, request reply | docker  |
| `it.ObservabilitySuite`                  | the traffic counters of the connections and the routes, the metrics, the lag of Kafka and Redis streams groups | docker  |
| `it.MqttReconnectSuite`                  | the mqtt client alone, against a mosquitto restarted on the same port | docker  |

The broker suites start their containers with [testcontainers](https://testcontainers.com/) and skip
themselves when no docker daemon is reachable. Each test publishes and consumes through real
Otoroshi routes. Tests named `.fail` pin a known defect: they pass while the defect is there and start
failing the day it is fixed, which is when the marker goes (there are none today).

```bash
sbt test                      # everything
sbt "testOnly *KafkaSuite"    # one broker
```

## Development

```bash
docker compose up -d          # every broker on its default port (kafka on 9093, kafka-ui on 8000)
sbt assembly                  # target/scala-3.8.4/otoroshi-event-brokers-extension-assembly_3-dev.jar
./rebuild.sh                  # assembly, linked into a local otoroshi checkout (../otoroshi/otoroshi/lib), reloaded
cd documentation && npm install && npm start   # the documentation site
```

The jar is an assembly: the RabbitMQ and MQTT clients are not on Otoroshi's classpath, the Kafka,
Pulsar, Redis and Artemis ones are.

CI runs every suite on each push and pull request; the documentation site is built and published to
GitHub Pages on each change under `documentation/`. Releases are made by hand:
**Actions → release → Run workflow**, with the version. The workflow tests, builds the jar, tags the
commit and publishes the release with the jar and its checksum.

What is planned and done is in [ROADMAP.md](ROADMAP.md).

## License

Apache 2.0
