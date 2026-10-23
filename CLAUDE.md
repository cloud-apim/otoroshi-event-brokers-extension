# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An Otoroshi admin extension that puts event brokers (Kafka, RabbitMQ, Pulsar, Redis pub/sub, MQTT)
behind Otoroshi routes: an `EventBrokerConnection` entity, and plugins that publish over HTTP and
consume as newline-delimited JSON, server-sent events or websockets.

**Requirements**: JDK 17+, Scala 3 (`scalaVersion` in `build.sbt`), Otoroshi 18 (provided). Docker for
the integration tests.

## Build Commands

```bash
sbt compile
sbt test                                   # unit + integration suites (sequential)
sbt "testOnly *KafkaSuite"                 # one suite
sbt "testOnly *KafkaSuite -- *partition*"  # tests of a suite matching a glob (MUnit filter)
sbt assembly                               # target/scala-3.8.4/otoroshi-event-brokers-extension-assembly_3-dev.jar
./rebuild.sh                               # assembly, linked into ../otoroshi/otoroshi/lib, hot reloaded
docker compose up -d                       # every broker on its default port, for manual testing
```

The released jar is the **assembly**: amqp-client and the shaded HiveMQ mqtt client (with rxjava2) are
shipped in it, while the Kafka
(pekko-connectors-kafka), Pulsar (pulsar4s) and Redis (lettuce) clients come from Otoroshi's classpath.

## Architecture

Sources live in `src/main/scala/com/cloud/apim/otoroshi/extensions/eventbrokers/`:

- **extension.scala** - `EventBrokersExtension` (`AdminExtension`): entity registration, state synced
  from the datastore in `syncStates`, backoffice assets (the UI page is inlined into `extension.js`)
- **entities/connections.scala** - `EventBrokerConnection` (kind + raw `settings` JSON, read lazily as
  `typedSettings`), its admin API resource (the `writeValidator` refuses settings that do not fit the
  kind) and its datastore. The entity format stays lenient on purpose: a stored connection with wrong
  settings must stay listed to be fixed
- **entities/settings.scala** - one settings case class per kind (`KafkaSettings`, `RabbitMQSettings`,
  `PulsarSettings`, `RedisSettings`, `MqttSettings`), read with `Fields`: snake_case, every error
  collected with its path, unknown fields refused, format checks skipped for `${vault://...}` values.
  `BrokerSettings.template(kind)` is what `_template?kind=` and the backoffice form start from. A new
  setting goes in the case class, its `read`, its `json`, the form (`EventBrokerConnectionsPage.js`)
  and `SettingsSuite`
- **manager.scala** - `ConnectionManager`, owned by the extension and fed by `syncStates`: one
  `ConnectionSlot` per connection, holding the `ConnectionHandle` serving requests and, while new
  settings connect, the next one (make before break). A handle connects in the background with backoff,
  healthchecks its client periodically (rebuilds it after too many failures), and once retired closes
  its client when the `ClientLease`s handed out are released or after the grace period. Plugins only
  ever `acquire` a lease (and release it, `lease.releasing(source)`); they never build a client
- **events.scala** - `EventBrokerConnectionEvent` (each state change) and
  `EventBrokerConnectionUnavailableAlert` (one per outage)
- **cloudevents.scala** - `CloudEvent` (structured json, batches, the http binary mode, the legacy
  format) and `CloudEventBindings`: how an event becomes a `BrokerMessage` on each kind (binary mode
  with `ce_` headers on kafka, pulsar and redis streams (fields of the entry), `cloudEvents:` application
  properties on rabbitmq, user properties on mqtt 5, structured payload on redis pub/sub and mqtt 3.1.1)
  and back, a message without cloudevents attributes being synthesized
  (position as id, `/<kind>/<topic>` as source), broker metadata added as `<kind><name>` extensions
- **api.scala** - `BrokerMessage`, a message as a broker carries it (key, data, headers, metadata:
  routing hints going out, what the broker says coming in), and the `EventBrokerClient` contract:
  - a client must not touch the network in its constructor: `start` / `healthcheck` / `close` run on
    the manager's io pool, bounded by timeouts. `BlockingIo` keeps that pool for the library calls that
    block (Kafka `send`, RabbitMQ publishes)
  - `publish(events, opts)`: one `PublishResult` per event, in order; a refused event is a failed
    result, never a failed future
  - `subscribe(spec)`: a `Future[Source[ReceivedEvent, ?]]` that fails before anything streams when
    the subscription cannot be opened (`IllegalArgumentException` = the call's fault, 400; anything
    else = the broker's, 502). Wrap the source with `Subscribed(source)(release)`: it releases on
    termination, and if the source is never run. `ReceivedEvent.ack()` is called by the plugins as the
    event is handed over (`PluginUtils.handOver`)
  - `capabilities` (acks, groups, replay, keys, headers): `PluginUtils.subscribe` refuses a spec the
    broker cannot serve. Keep them and the readme's table in sync
- **clients/** - one `EventBrokerClient` per broker kind; redis has two, pub/sub (redis.scala) and
  streams (redisstreams.scala), picked by the `mode` of the connection
- **plugins/** - `HttpEventsIngester`, `HttpEventsConsumer` (http.scala), `SseEventConsumer` (sse.scala),
  `IngesterConsumerWebsocket` (ws.scala); events.scala reads requests into events (`Ingest`), writes
  them (`Output`, `OutputFormat`: cloudevents or raw) and makes the SSE ids (`SseIds`: kafka positions
  for `Last-Event-ID` resume); utils.scala holds the shared plugin config
  (`EventBrokerPluginConfig`: `options` fixed by the route and evaluated with the expression language,
  `allowed_overrides`, `allowed_topics`), `CallOptions` (the policy: a caller option not allowed is a
  400, caller values are never evaluated, per-event routing is checked too), `subscriptionSpec`
  (options to `SubscriptionSpec`), `subscribe`, `handOver` and `publishAll` (batches of 100 / 10 ms).
  ws.scala holds `InFlight`, the client acks of the websocket (`acks=client`, `max_in_flight`).
  Test routes allow every option (`Gateway.everythingAllowed`); `PolicySuite` tests the policy
- **src/main/resources/cloudapim/extensions/event-brokers/** - backoffice page (plain JS, no build) and icon

Source paths don't always match packages: the extension entry point and the plugins are declared in
`otoroshi_plugins.com.cloud.apim...` packages (Otoroshi's classpath scanning only looks there), the rest
in `com.cloud.apim.otoroshi.extensions.eventbrokers...`.

## Testing

- `src/test/scala/.../eventbrokers/*Suite.scala` - unit suites, no infrastructure.
- `it/ConnectionsApiSuite` covers what the admin api accepts (no docker).
- `it/ConnectionManagerSuite` drives a `ConnectionManager` of its own over `FakeClient`s (no docker):
  the place for lifecycle rules. `it/ConnectionsSuite` checks the same against a real Redis (paused
  with `docker pause`, connections counted with `INFO clients`).
- `src/test/scala/.../eventbrokers/it/` - integration suites. `Gateway` boots one real Otoroshi in the
  test jvm (in-memory storage, extension enabled) shared by every suite; `Brokers` starts one
  testcontainers container per broker, lazily, shared too. Each suite creates a connection and routes
  through the admin API and calls the routes over HTTP (pekko-http, one TCP connection per call, so
  ending a stream hangs up on the gateway like a real client would). `BrokerSuite` skips the suite
  when no docker daemon is reachable.
- Pub/sub brokers (Redis pub/sub, MQTT, Pulsar from latest) only deliver once the subscription is in
  place and the gateway gives no signal for it: use `keepPublishing` + `assertConsecutive` rather than
  sleeps. Where the broker keeps what it got (Kafka, Redis streams), publish first and read `from=earliest`.
- **Tests named `.fail` pin a known defect**: they pass while it is there and fail once it is fixed.
  When you fix one, remove the marker (and its comment). Check a `.fail` test fails for the reason its
  comment gives, not because of the harness.
- `build.sbt` needs `Test / classLoaderLayeringStrategy := Flat` (classpath scanning) and the netty /
  play-json `dependencyOverrides` (Otoroshi's own overrides don't travel through its pom). Logs go
  through `src/test/resources/logback-test.xml`, forced with `logger.resource` because Play otherwise
  loads the `logback.xml` of the Otoroshi jar.

## CI and releases

- `.github/workflows/ci.yaml` runs `sbt test` on every push to main and every pull request (docker is
  available on the runners).
- `.github/workflows/release.yaml` is started by hand with a version: tests, assembly, tag, GitHub
  release with the jar and its sha-256.
