---
title: Consuming
description: Subscriptions, groups, delivery guarantees, replay, and what each broker can do.
---

# Consuming

The consumers subscribe once the call comes in, and answer an error before streaming anything when
they cannot: `400` for what the call got wrong (no topic, an option the broker does not support), `502`
for what the broker refused (a queue that does not exist), `503` while the connection is not connected.
Everything the subscription holds on the broker (a consumer, a channel, a subscription, a temporary
queue) is released when the client goes away.

The **HTTP events consumer** streams `application/x-ndjson`, one event per line; `heartbeat=<millis>`
adds an empty line when nothing happens. `format=cloudevents` (the default) gives the structured
events, `format=raw` their data alone.

## Options

| Option | Means |
|---|---|
| `topic`, `topics`, `queue`, `channel`, `stream`, `address` | what to read. The default of the connection without |
| `group` (or `group_id`, `subscription`) | share the events with the other subscribers of the group, instead of getting them all |
| `from` | `latest` (the default) or `earliest` |
| `from_timestamp` | from that date (epoch millis) |
| `offset_from` | from that offset (Kafka: `partition` for one partition, `offset_to` to stop; RabbitMQ streams) |
| `from_id` | from that entry id, included (Redis streams) |
| `delivery` | `at-most-once` (the default) or `at-least-once` |
| `take`, `drop`, `duration` | how many events to stream, to skip, for how long (millis) |
| `format` | `cloudevents` or `raw` |

All the options, broker by broker, are in the [reference](../reference/options.md).

## Delivery

- `at-most-once`: an event is acked as it leaves the subscription. What a client did not get (it went
  away, the stream was cut) is not delivered again.
- `at-least-once`: an event is acked once it is handed over to the response. What a client did not get
  is delivered again, to it or to another member of its group: possibly twice.

With the [websocket plugin](./sse-and-websockets.md#websockets), the client acks the events itself.

## What each broker can do

Anything else is refused with a `400`.

|   | Kafka | RabbitMQ | Pulsar | Redis pub/sub | Redis streams | MQTT | Artemis |
|---|---|---|---|---|---|---|---|
| without a group | a consumer group of its own: every event | consumers of a queue share it; `exchange` + `routing_key`: a queue of its own, every message | a non durable subscription of its own: every message | every message | `XREAD`: every entry | every message | multicast: a temporary queue of its own, every message. anycast: the consumers of the queue share it |
| `group` | consumer group | no (a queue already is one) | durable subscription, shared (`subscription_type`) | no | consumer group (`XREADGROUP`), created where `from` says | MQTT 5: a shared subscription (`$share/<group>/<topic>`) | multicast: a durable queue named after the group, shared |
| `at-least-once` | commits what was handed over | acks, the rest goes back to the queue | acks, the rest is delivered again | no | in a group: `XACK` once handed over, the rest goes to another member | no | acks, the rest goes back to the queue |
| `from` / replay | `earliest`, `from_timestamp`, `offset_from` | streams only: `earliest`, `from_timestamp`, `offset_from` | `earliest` (new group), `from_timestamp` | no | `earliest`, `from_timestamp`, `from_id` | no | no |

The broker pages say more: [Kafka](../connections/kafka.md), [RabbitMQ](../connections/rabbitmq.md),
[Pulsar](../connections/pulsar.md), [Redis](../connections/redis.md), [MQTT](../connections/mqtt.md),
[Artemis](../connections/artemis.md).

## Pub/sub brokers

Redis pub/sub, MQTT, and the subscriptions of Pulsar from `latest` only get what is published once
they are in place. A client that subscribes and publishes right away may miss its first events: on
those brokers, subscribe first, or use a broker that keeps what it gets (Kafka, Redis streams, a
RabbitMQ queue, an Artemis group or anycast queue).
