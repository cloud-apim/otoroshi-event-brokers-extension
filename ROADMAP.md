# Roadmap

Where the extension is going, and where it is. The goal: a production-grade product that works
perfectly, honors what each broker does best, is tested against real brokers, documented, and
observable through Otoroshi.

Updated with every pull request. `[x]` done, `[~]` in progress, `[ ]` to do.

## Decisions

Taken on 2026-10-22, not to be discussed again:

| Topic | Decision |
|---|---|
| Compatibility | the entity and plugin formats change **without migration**: connections of the prototype are recreated |
| Caller options (`x-ca-event-broker-opts-*`) | **refused by default**: the route says what its callers may ask (`allowed_overrides`, `allowed_topics`) |
| Event envelope | **CloudEvents 1.0**, with the protocol binding of each broker |
| MQTT client | **HiveMQ** (MQTT 5 and 3.1.1), instead of Paho |
| New broker | **ActiveMQ Artemis** |
| Webhooks | a **Redis shared between the nodes** (Otoroshi's stateful clients API), always required except on a single instance |
| Name | **otoroshi-event-brokers-extension** (the extension id and its configuration keys stay as they are) |

## Phase 0: foundations

- [x] Build: Otoroshi 18 provided, netty and play-json overrides, assembly
  ([#2](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/2))
- [x] Test harness: one Otoroshi in the test jvm, one testcontainers container per broker, `.fail` tests pinning
  the known defects ([#2](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/2))
- [x] CI on every push and pull request, release workflow (jar and its sha-256) ([#2](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/2))

## Phase 1: the core

- [x] Connection manager: clients connected in the background with backoff, healthchecks, rebuilt after
  failures, make before break on new settings, leases, status api, connection events and outage alerts
  ([#3](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/3))
- [x] Typed settings per kind, validated on write, per-kind form from `_template`
  ([#4](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/4))
- [x] Client contract v2: one result per published event, subscriptions released with their stream, acks
  and delivery modes, capabilities; the five clients rewritten ([#5](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/5))
- [x] CloudEvents envelope and bindings, output formats, server-sent events framing, heartbeat, Kafka
  `Last-Event-ID` resume ([#6](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/6))
- [x] Options policy (route options with the expression language, allowed overrides and topics), local
  plugin config, websocket acks ([#7](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/7))
- [x] Every defect pinned by a `.fail` test fixed (none left)

## Phase 2: each broker at its best

- [x] MQTT 5 and 3.1.1 on HiveMQ: shared subscriptions, user properties, websockets, reconnection
  ([#8](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/8))
- [x] Redis streams: consumer groups, acks, claims of abandoned entries, replay, SSE resume
  ([#9](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/9))
- [x] ActiveMQ Artemis on its core protocol: multicast and anycast, durable shared subscriptions, message
  groups, selectors, duplicate detection, delayed delivery ([#10](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/10))
- [x] TLS for every broker, with the certificates of the Otoroshi store, tested with real certificates
  (mutual TLS) against each broker: Kafka (inline PEM, no files), RabbitMQ, Pulsar, Redis, MQTT, Artemis
- [x] Authentication tested on every broker: Kafka SASL SCRAM (and PLAIN), Pulsar tokens, users and passwords
  of RabbitMQ, Redis (ACL), MQTT and Artemis; the Kafka JAAS configuration is built from the mechanism
- [x] Extras:
  - [x] Pulsar: SSE resume by message id, partitioned topics included
  - [x] RabbitMQ: declarative topology (exchanges, queues of every type, bindings), RabbitMQ streams (replay
    from the first message, an offset, a date; SSE resume)
  - [x] Kafka: producer tuning (acks, idempotence, compression, batching, linger)

## Phase 3: features on top

- [x] Webhooks: events pushed to HTTP endpoints (structured, binary or raw, signed), with retries, dead
  letters and alerts, delivered by one node at a time, decided in a Redis the nodes share (Otoroshi's
  stateful clients API), required except on a single instance; backoffice page and status api
- [x] Admin api and backoffice to try a connection: publish a test event, peek at what a topic or queue holds
  (never acking: what is peeked at stays where it was)
- [x] Data exporter: Otoroshi events (analytics, alerts, audit) exported to any connection, as CloudEvents
- [x] Workflow functions: publish and consume from Otoroshi workflows
- [x] Request-reply over every broker (`correlationid` and `replyto` extensions, plus the native fields of
  RabbitMQ and MQTT 5)

## Phase 4: observability (ongoing)

- [x] Connection state events (`EventBrokerConnectionEvent`) and one alert per outage
  (`EventBrokerConnectionUnavailableAlert`) ([#3](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/3))
- [x] Connections per state as metrics, status api, status badges in the backoffice
  ([#3](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/3))
- [x] Metrics per connection and route: published, failed, consumed, acked, publish latency (in the status api,
  the backoffice list and otoroshi's metrics)
- [x] Open subscriptions, and consumer lag where the broker tells it (Kafka, Redis streams)

## Phase 5: documentation

- [ ] Docusaurus site in `documentation/`, built and published to GitHub Pages by a workflow (the way
  otoroshi-clevercloud-reaper does it)
- [ ] A getting started per broker, the plugins, the options policy, the CloudEvents bindings, operations
  (status, events, metrics)
- [ ] Readme reduced to an overview pointing to the site (its status note about `.fail` tests is outdated)

## Housekeeping

- [x] Repository and artifacts renamed otoroshi-event-brokers-extension
  ([#11](https://github.com/cloud-apim/otoroshi-event-brokers-extension/pull/11))
- [ ] Backoffice forms checked in a real browser (so far only smoke-tested with node)
