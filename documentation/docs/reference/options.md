---
title: Options
description: Every option of a publish and of a subscription, broker by broker.
---

# Options

The options of a call come from the `options` of the route's plugin (with expressions), and from the
caller (query parameters and headers prefixed with `x-ca-event-broker-opts-`) for those the route
allows (see [The plugins](../routes/plugins.md)). The admin API's [try-it routes](../features/try-a-connection.md),
webhooks and workflow functions take the same options, without the prefix.

## Publishing

| Option | Brokers | |
|---|---|---|
| `topic` | Kafka, Pulsar, MQTT, and as a fallback everywhere | where to |
| `queue` | RabbitMQ (the default exchange), Artemis (anycast) | |
| `exchange`, `routing_key` | RabbitMQ | |
| `channel` | Redis pub/sub | |
| `stream` | Redis streams | |
| `address`, `routing_type` | Artemis | `multicast` or `anycast` |
| `key` | Kafka (record key), Artemis (group id) | an event's `partitionkey` is its key too |
| `partition` | Kafka | |
| `qos`, `retained` | MQTT | |
| `priority`, `ttl`, `delay`, `deliver_at`, `durable` | Artemis | |
| `source`, `type`, `subject` | all | the attributes a request does not give |
| `lines` | all | read the body line by line |

## Subscribing

| Option | Brokers | |
|---|---|---|
| `topic`, `topics` | Kafka, Pulsar, MQTT, everywhere | `topics`: comma separated |
| `queue` | RabbitMQ, Artemis (anycast) | |
| `exchange`, `routing_key` | RabbitMQ | a queue of its own, bound to them |
| `channel` | Redis pub/sub | glob patterns allowed |
| `stream` | Redis streams | |
| `address`, `routing_type`, `selector` | Artemis | `address::queue` for a queue |
| `group` (`group_id`, `subscription`) | Kafka, Pulsar, Redis streams, MQTT 5, Artemis (multicast) | |
| `subscription_type` | Pulsar | `exclusive`, `shared`, `failover`, `key_shared` |
| `consumer` | Redis streams | a named consumer of the group |
| `from` | Kafka, Pulsar, Redis streams, RabbitMQ streams | `latest`, `earliest` |
| `from_timestamp` | Kafka, Pulsar, Redis streams, RabbitMQ streams | epoch millis |
| `offset_from`, `offset_to`, `partition` | Kafka (`offset_from` on RabbitMQ streams too) | |
| `from_id` | Redis streams | an entry id, included |
| `stream` (`true`) | RabbitMQ | the queue is a stream |
| `delivery` | all with acks | `at-most-once`, `at-least-once` |
| `prefetch` | RabbitMQ, Redis streams, Artemis | 100 |
| `qos` | MQTT | |
| `client_id`, `group_instance_id` | Kafka | |

## Shaping the response

| Option | |
|---|---|
| `take`, `drop`, `duration` | how many events to stream, to skip, for how long (millis) |
| `format` | `cloudevents` (the default) or `raw` |
| `heartbeat` | millis: an empty line (HTTP consumer), a comment (SSE, 15000 by default, `0` to stop) |
| `acks`, `max_in_flight` | websocket: `acks=client`, and how many events wait for theirs (100) |
| `publish_results` | websocket: answer every frame published with its result |

`take`, `drop`, `duration`, `format` and `heartbeat` are the only options a caller may set by default.

## Request reply

| Option | |
|---|---|
| `reply_to` | where the replies go: required |
| `reply_options` | how to read them, as JSON |
| `timeout` | millis, 10000 |
