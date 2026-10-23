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
| `redis`    | Redis pub/sub, or Redis streams | lettuce (otoroshi)              |
| `mqtt`     | any MQTT 5 or 3.1.1 broker, over tcp or websockets | HiveMQ MQTT client      |
| `artemis`  | ActiveMQ Artemis (core protocol) | artemis core client (otoroshi) |

### Plugins

| Plugin                                      | Identifier                                                                                   | Does                                                  |
|---------------------------------------------|----------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Cloud APIM - HTTP events ingester           | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsIngester`        | publishes the request body (one event, a batch, or newline-delimited) |
| Cloud APIM - HTTP events consumer           | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsConsumer`        | streams events as newline-delimited JSON              |
| Cloud APIM - SSE events consumer            | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.SseEventConsumer`          | streams events as server-sent events                  |
| Cloud APIM - Websocket Ingester/Consumer    | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.IngesterConsumerWebsocket` | publishes incoming frames, streams events back        |

Every plugin points to an **event broker connection**, and decides what its callers may ask:

```json
{
  "ref": "connection_...",
  "options": { "topic": "orders-${apikey.metadata.tenant}", "group": "billing" },
  "allowed_overrides": ["take", "drop", "duration", "format", "heartbeat"],
  "allowed_topics": [],
  "include_query": true,
  "include_headers": true
}
```

- `options` are what the route fixes: the options below, the topic first. Their values may hold
  Otoroshi [expressions](https://www.otoroshi.io/docs/topics/expression-language) (`${apikey.metadata.tenant}`,
  `${req.headers.x-tenant}`, `${user.email}`), evaluated for each call.
- A caller sets options with query parameters and headers prefixed with `x-ca-event-broker-opts-`
  (`?x-ca-event-broker-opts-take=10`), but **only those in `allowed_overrides`**: anything else is refused
  with a `400`. By default a caller can only shape its own response (`take`, `drop`, `duration`,
  `format`, `heartbeat`); the topic, the group, the start position, the delivery stay the route's.
  `["*"]` allows everything. What a caller sends is never evaluated as an expression.
- When the caller may name the topic (queue, channel), `allowed_topics` holds the glob patterns it must
  match (`["orders-*"]`); empty, any.
- Where an event says it goes (the `vendor_metadata` of the legacy format) is held to the same rules.

### Events are CloudEvents

Every event going in or out is a [CloudEvents 1.0](https://cloudevents.io/) event, whatever the broker.

**What the ingester takes**, by `Content-Type`:

| Content type | Is |
|---|---|
| `application/cloudevents+json` | one structured event |
| `application/cloudevents-batch+json` | a JSON array of structured events |
| anything else | one event in binary mode: the body is its data, the content type is its `datacontenttype` (charset included), its attributes come from `ce-` headers (`ce-id`, `ce-type`, `ce-source`, `ce-subject`, `ce-<extension>`) |
| `application/vnd.cloud-apim-event-broker+json` | the format of the extension before CloudEvents, still read |

A chunked body (or the `lines=true` option) is read line by line, each line one event. What a request
does not say is filled in: a fresh `id`, `/otoroshi/routes/<route id>` as `source` (or the `source`
option), `com.cloud-apim.event-brokers.event` as `type` (or the `type` option), now as `time`.

**How events travel on each broker** (the CloudEvents protocol bindings):

| Broker | Mode | Where the attributes are |
|---|---|---|
| Kafka | binary | `ce_*` headers, the data as the value, `content-type` header, `partitionkey` as the record key |
| Pulsar | binary | `ce_*` properties, the data as the payload, `partitionkey` as the message key |
| RabbitMQ | binary | `cloudEvents:*` application properties, the amqp `content-type` |
| MQTT 5 | binary | user properties (no prefix), the mqtt 5 content type |
| Artemis | binary | `cloudEvents_*` properties, `cloudEvents_datacontenttype` included (the JMS flavour of the AMQP binding: JMS clients and selectors read them), the data as the body of a bytes message, `partitionkey` as the group id |
| Redis streams | binary | `ce_*` fields of the entry, `content-type` field, the data in the `data` field |
| Redis pub/sub, MQTT 3.1.1 | structured | no headers: the whole event as JSON in the payload |

So a consumer outside Otoroshi (a Kafka consumer, a CloudEvents SDK) reads what the gateway publishes,
and what another producer published is read as an event too: its `id` is its position when the broker
has one (`<topic>-<partition>-<offset>` on Kafka, `<stream>-<entry id>` on Redis streams), its `source` `/<kind>/<topic>`, its `type`
`com.cloud-apim.event-brokers.message`, its content type guessed from the payload when nothing says it.
Every event read carries what the broker says of the message as extensions: `kafkatopic`,
`kafkapartition`, `kafkaoffset`, `rabbitmqroutingkey`, `rabbitmqredelivered`, `pulsarmessageid`,
`redischannel`, `redisstream`, `redisid`, `redisredelivered`, `mqtttopic`, `mqttqos`, `artemisaddress`,
`artemisqueue`, `artemisdeliverycount`, ...

A Redis stream entry added by another producer without a `data` field is read with all its fields as
a JSON object (`XADD orders * user 42` gives `{"user": "42"}`).

**What the consumers give**: the structured events (`format=cloudevents`, the default), or their data
alone (`format=raw`).

### Publishing

The ingester answers one JSON line per event, in order, with the id of the event, once the broker has
taken it or refused it:

```json
{"published": true, "id": "4f1c...", "details": {"topic": "orders", "partition": 0, "offset": 42}}
{"published": false, "id": "77ab...", "error": "publish_error", "error_description": "no queue got the message: ..."}
{"published": false, "error": "invalid_event", "error_description": "source is required"}
```

A broker refusing one event does not fail the others. Kafka and Pulsar give the position of the
message, RabbitMQ waits for the broker's confirm and reports the messages no queue gets (instead of
dropping them), Redis pub/sub gives how many subscribers got the message, Redis streams the id of the
entry, Artemis waits for the broker to take each message and gives its JMS message id. A body that is not
valid at all is refused with a `400`.

Publish options: `topic` (`queue`, `exchange` and `routing_key` for RabbitMQ, `channel` or `stream` for Redis,
`address` for Artemis), `key` and `partition` for Kafka, `qos` and `retained` for MQTT, `routing_type`,
`key` (the group id), `priority`, `ttl`, `delay` or `deliver_at` (epoch millis) and `durable` for Artemis,
`source`, `type` and `subject` for the attributes the request does not give.

### Consuming

The consumers subscribe once the call comes in, and answer an error before streaming anything when
they cannot: `400` for what the call got wrong (no topic, an option the broker does not support), `502`
for what the broker refused (a queue that does not exist), `503` while the connection is not connected.
Everything the subscription holds on the broker (consumer, channel, subscription) is released when the
client goes away.

| Option | Means |
|---|---|
| `topic`, `topics`, `queue`, `channel`, `stream`, `address` | what to read. The default of the connection without |
| `group` (or `group_id`, `subscription`) | share the events with the other subscribers of the group, instead of getting them all |
| `from` | `latest` (the default) or `earliest` |
| `from_timestamp` | from that date (epoch millis) |
| `offset_from` | from that offset (Kafka; `partition` for one partition, `offset_to` to stop) |
| `from_id` | from that entry id, included (Redis streams) |
| `delivery` | `at-most-once` (the default): acked as received. `at-least-once`: acked once handed over to the response, what a client did not get is delivered again |
| `take`, `drop`, `duration` | how many events to stream, to skip, for how long (millis) |

What each broker can do (anything else is refused with a `400`):

|   | Kafka | RabbitMQ | Pulsar | Redis pub/sub | Redis streams | MQTT | Artemis |
|---|---|---|---|---|---|---|---|
| without a group | a consumer group of its own: every event | consumers of a queue share it; `exchange` + `routing_key`: a queue of its own, every message | a non durable subscription of its own: every message | every message | `XREAD`: every entry | every message | multicast: a temporary queue of its own, every message. anycast: the consumers of the queue share it |
| `group` | consumer group | no (a queue already is one) | durable subscription, shared (`subscription_type`: `exclusive`, `shared`, `failover`, `key_shared`) | no | consumer group (`XREADGROUP`), created where `from` says if it does not exist | MQTT 5: a shared subscription (`$share/<group>/<topic>`) | multicast: a durable queue named after the group, shared (a JMS shared durable subscription) |
| `at-least-once` | commits what was handed over | acks, the rest goes back to the queue | acks, the rest is delivered again | no | in a group: `XACK` once handed over, the rest goes to another member | no | acks, the rest goes back to the queue (a group on a multicast address) |
| `from` / replay | `earliest`, `from_timestamp`, `offset_from` | no | `earliest` (new group), `from_timestamp` | no | `earliest`, `from_timestamp`, `from_id` | no | no |

Other options: `prefetch` (RabbitMQ, Redis streams and Artemis, 100), `qos` (MQTT), `client_id` and
`group_instance_id` (Kafka), `consumer` (Redis streams), `routing_type` and `selector` (Artemis).

**Redis streams groups**: an entry read by a member is its own until acked: as it leaves the
subscription (`at-most-once`) or once handed over (`at-least-once`). What a member read and left unacked
for `claim_min_idle_ms` (it went away, it was cut) is claimed by another member (`XAUTOCLAIM`) and
delivered again, with `redisredelivered`. Each subscription is a consumer of its own, removed from the
group when it goes away with nothing pending; `consumer=<name>` names it instead: a named consumer
coming back gets what it had pending first (one subscription per name). A smaller `prefetch` shares the
entries more evenly between slow members.

**Artemis addresses**: what is sent to a multicast address goes to every queue bound to it, what is
sent to an anycast address goes to its queue (named after it). `routing_type` says which one an address
is (`queue` instead of `address` means anycast); without it, the connection's `default_routing_type`.
Addresses and queues are made as the broker's address settings allow (`auto-create-*`), and
`address::queue` reads, or sends to, that very queue. `selector` is an Artemis filter
(`cloudEvents_type = 'order.created' AND priority > 4`). Messages with the same key (their group id) go
to one consumer at a time, in order. A consumer that closes gives back what it did not ack as not
delivered: only a consumer that fails counts its messages as delivered (`artemisdeliverycount`, and
`max-delivery-attempts` before the dead letters).

The HTTP consumer streams `application/x-ndjson` (one event per line); `heartbeat=<millis>` adds an
empty line when nothing happens. The **SSE consumer**:

- names each event after its `type` (`event:`), and gives every line of the data its own `data:` field
- sends a `: keep-alive` comment every 15 seconds without events (`heartbeat=<millis>`, `0` to stop)
- gives each event an `id:`. On Kafka and Redis streams it holds the positions read (on every partition,
  in every stream): a client coming back with it in `Last-Event-ID` (what `EventSource` does by itself)
  resumes right after what it got. In a Redis streams group, the group knows where it is: the position is
  not used
- answers with `Cache-Control: no-cache` and `X-Accel-Buffering: no`, so that no proxy holds the events

The **websocket plugin** has an `in` and an `out` configuration (either can be left without a `ref`):
frames sent by the client are published one at a time (a text frame holding a structured CloudEvent is
that event), events consumed are sent as frames. With `acks=client` (on `out`), an event is acked only
when the client answers `{"ack": "<event id>"}` (or a list of ids), and no more than `max_in_flight`
(100) wait for their ack: with `delivery=at-least-once`, what the client did not ack is delivered again.
With `publish_results=true` (on `in`), every frame published is answered with its result.

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

A connection is a `kind` and its `settings`:

```json
{ "id": "connection_...", "name": "orders", "kind": "kafka", "settings": { "bootstrap_servers": ["kafka:9092"] } }
```

The admin API refuses settings that do not fit their kind, with every field at fault
(`settings.bootstrap_servers: required`, `settings.servers: unknown field`, ...). Unknown fields are
errors: a typo is never silently ignored. `GET .../event-broker-connections/_template?kind=<kind>` gives
a valid starting point. Any string can be a [vault](https://www.otoroshi.io/docs/topics/secrets)
reference (`"password": "${vault://local/brokers/kafka_password}"`): the admin API keeps the reference,
the clients get the secret.

A connection whose stored settings are wrong (written by hand, or in the format of an older version)
stays listed so that it can be fixed; it is `unavailable` with the errors, and is not tried again
until its settings change.

<details>
<summary>The settings of each kind, with their defaults</summary>

**kafka** (`bootstrap_servers` is required)

```json
{
  "bootstrap_servers": ["kafka-1:9092", "kafka-2:9092"],
  "default_topic": null,
  "security_protocol": "PLAINTEXT",
  "sasl": { "mechanism": "PLAIN", "username": null, "password": null, "jaas_config": null },
  "tls": {
    "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true,
    "keystore_path": null, "truststore_path": null, "store_password": null
  },
  "properties": {}
}
```

`security_protocol` is `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`; the `SASL_*` ones need a
username and a password, or a `jaas_config`. TLS uses certificates of the Otoroshi store
(`client_certs`, `trusted_certs`: certificate ids), or keystore and truststore files. `properties` are
raw Kafka client properties, applied last.

**rabbitmq** (`addresses` is required)

```json
{ "addresses": ["rabbit:5672"], "virtual_host": "/", "username": null, "password": null, "default_queue": null }
```

**pulsar** (`service_url` is required: `pulsar://` or `pulsar+ssl://`)

```json
{
  "service_url": "pulsar://pulsar:6650",
  "tenant": "public",
  "namespace": "default",
  "default_topic": null,
  "auth": { "kind": "none" },
  "tls": {
    "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true,
    "trust_certs_file_path": null
  }
}
```

`auth` is `{"kind": "none"}`, `{"kind": "token", "token": "..."}` or
`{"kind": "basic", "username": "...", "password": "..."}`. Topics without a scheme are
`persistent://<tenant>/<namespace>/<topic>`.

**redis** (`uri` is required: `redis://`, `rediss://` for TLS)

```json
{
  "uri": "redis://redis:6379",
  "username": null,
  "password": null,
  "mode": "pubsub",
  "default_channel": null,
  "streams": {
    "default_stream": null,
    "max_length": null,
    "exact_trimming": false,
    "data_field": "data",
    "claim_min_idle_ms": 60000
  },
  "tls": { "start_tls": false, "verify_peer": true },
  "io_thread_pool_size": null,
  "computation_thread_pool_size": null
}
```

`mode` is `pubsub` (channels: what is published reaches the subscribers there are, and no one else) or
`streams` (an append-only log: replay, consumer groups, acks); `streams` only applies to the latter.
`max_length` trims the stream on each publish, to about that many entries (`MAXLEN ~`), exactly with
`exact_trimming` (`0` or `null`: no limit). `data_field` is the field of an entry that holds the data,
the other ones are its headers. `claim_min_idle_ms` (`0`: never) is how long an entry may stay unacked by
a member of a group before another one gets it.

**mqtt** (`host` is required)

```json
{
  "host": "mosquitto",
  "port": 1883,
  "version": 5,
  "transport": "tcp",
  "websocket_path": "/mqtt",
  "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true },
  "client_id_prefix": "otoroshi",
  "username": null,
  "password": null,
  "default_topic": null,
  "default_qos": 0,
  "retain": false,
  "clean_session": true,
  "session_expiry_seconds": 0,
  "connection_timeout_seconds": 10,
  "keep_alive_seconds": 60
}
```

`version` is `5` or `3` (MQTT 3.1.1, no properties, no shared subscriptions). `transport` is `tcp` or
`websocket`. A clean session loses its subscriptions with its connection: they are made again once the
client is connected again. Every client gets its own id, the prefix and a random suffix: two nodes of a cluster never share one
(a broker drops the older of two clients with the same id).

**artemis** (`url` is required)

```json
{
  "url": "tcp://artemis:61616",
  "username": null,
  "password": null,
  "default_address": null,
  "default_routing_type": "multicast",
  "durable": true,
  "duplicate_detection": false,
  "consumer_window_size": 1048576,
  "reconnect_attempts": -1,
  "call_timeout_ms": 10000
}
```

`url` is an Artemis core url: a list of brokers (`(tcp://a:61616,tcp://b:61616)?ha=true`), and its
netty parameters for TLS (`sslEnabled=true;trustStorePath=...`). `duplicate_detection` gives each message
the source and the id of its event as duplicate id (`_AMQ_DUPL_ID`): the broker drops an event
published twice. `consumer_window_size` is what a consumer buffers ahead, in bytes. The client
reconnects by itself `reconnect_attempts` times (`-1`: forever); while it does, a call waits
`call_timeout_ms` at most.

</details>

A call that names no topic (queue, channel) uses the default one of its connection, and is refused with
a `400` when there is none (a publish gets a failed result for each event).

## Running the tests

| Suites                                   | What they run against                                        | Needs   |
|------------------------------------------|--------------------------------------------------------------|---------|
| `CloudEventSuite`, `PluginConfigSuite`, `ConnectionManagerSettingsSuite`, `SettingsSuite` | the code alone | nothing |
| `it.ConnectionManagerSuite`              | the connection lifecycle over fake brokers, on the env of an Otoroshi in the test jvm | nothing |
| `it.ConnectionsApiSuite`                 | what the admin api accepts as connections, on an Otoroshi in the test jvm | nothing |
| `it.PolicySuite`                         | what a caller may ask under the configuration of its plugin, against Kafka | docker  |
| `it.ConnectionsSuite`                    | the connection lifecycle against a real Redis: not there yet, paused, back, settings changed, deleted | docker  |
| `it.KafkaSuite`, `it.RabbitMQSuite`, `it.PulsarSuite`, `it.RedisSuite`, `it.RedisStreamsSuite`, `it.MqttSuite`, `it.ArtemisSuite` | an Otoroshi in the test jvm, with the extension loaded, and a real broker in a container | docker  |
| `it.MqttReconnectSuite`                  | the mqtt client alone, against a mosquitto restarted on the same port | docker  |

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
