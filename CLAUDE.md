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
sbt assembly                               # target/scala-3.8.4/otoroshi-extension-event-brokers-assembly_3-dev.jar
./rebuild.sh                               # assembly, linked into ../otoroshi/otoroshi/lib, hot reloaded
docker compose up -d                       # every broker on its default port, for manual testing
```

The released jar is the **assembly**: amqp-client and paho are shipped in it, while the Kafka
(pekko-connectors-kafka), Pulsar (pulsar4s) and Redis (lettuce) clients come from Otoroshi's classpath.

## Architecture

Sources live in `src/main/scala/com/cloud/apim/otoroshi/extensions/eventbrokers/`:

- **extension.scala** - `EventBrokersExtension` (`AdminExtension`): entity registration, state synced
  from the datastore in `syncStates`, backoffice assets (the UI page is inlined into `extension.js`)
- **entities/connections.scala** - `EventBrokerConnection` (kind + free-form `settings` JSON), its admin
  API resource and its datastore
- **manager.scala** - `ConnectionManager`, owned by the extension and fed by `syncStates`: one
  `ConnectionSlot` per connection, holding the `ConnectionHandle` serving requests and, while new
  settings connect, the next one (make before break). A handle connects in the background with backoff,
  healthchecks its client periodically (rebuilds it after too many failures), and once retired closes
  its client when the `ClientLease`s handed out are released or after the grace period. Plugins only
  ever `acquire` a lease (and release it, `lease.releasing(source)`); they never build a client
- **events.scala** - `EventBrokerConnectionEvent` (each state change) and
  `EventBrokerConnectionUnavailableAlert` (one per outage)
- **api.scala** - `EventWrapper`, the broker-neutral event (key, data, content types, headers, vendor
  metadata) with its JSON / SSE / websocket serializations, and the `EventBrokerClient` trait. A client
  must not touch the network in its constructor: `start` / `healthcheck` / `close` run on the
  manager's blocking pool, bounded by timeouts
- **clients/** - one `EventBrokerClient` per broker kind
- **plugins/** - `HttpEventsIngester`, `HttpEventsConsumer` (http.scala), `SseEventConsumer` (sse.scala),
  `IngesterConsumerWebsocket` (ws.scala); utils.scala holds the shared plugin config and the per-call
  options read from `x-ca-event-broker-opts-*` query params and headers
- **src/main/resources/cloudapim/extensions/event-brokers/** - backoffice page (plain JS, no build) and icon

Source paths don't always match packages: the extension entry point and the plugins are declared in
`otoroshi_plugins.com.cloud.apim...` packages (Otoroshi's classpath scanning only looks there), the rest
in `com.cloud.apim.otoroshi.extensions.eventbrokers...`.

## Testing

- `src/test/scala/.../eventbrokers/*Suite.scala` - unit suites, no infrastructure.
- `it/ConnectionManagerSuite` drives a `ConnectionManager` of its own over `FakeClient`s (no docker):
  the place for lifecycle rules. `it/ConnectionsSuite` checks the same against a real Redis (paused
  with `docker pause`, connections counted with `INFO clients`).
- `src/test/scala/.../eventbrokers/it/` - integration suites. `Gateway` boots one real Otoroshi in the
  test jvm (in-memory storage, extension enabled) shared by every suite; `Brokers` starts one
  testcontainers container per broker, lazily, shared too. Each suite creates a connection and routes
  through the admin API and calls the routes over HTTP (pekko-http, one TCP connection per call, so
  ending a stream hangs up on the gateway like a real client would). `BrokerSuite` skips the suite
  when no docker daemon is reachable.
- Pub/sub brokers (Redis, MQTT, Pulsar from latest) only deliver once the subscription is in place and
  the gateway gives no signal for it: use `keepPublishing` + `assertConsecutive` rather than sleeps.
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
