---
title: Publishing
description: What the ingester takes, and what it answers.
---

# Publishing

The **HTTP events ingester** publishes the body of the request.

## What it takes

By `Content-Type`:

| Content type | Is |
|---|---|
| `application/cloudevents+json` | one structured event |
| `application/cloudevents-batch+json` | a JSON array of structured events |
| anything else | one event in binary mode: the body is its data, the content type is its `datacontenttype` (charset included), its attributes come from `ce-` headers (`ce-id`, `ce-type`, `ce-source`, `ce-subject`, `ce-<extension>`) |
| `application/vnd.cloud-apim-event-broker+json` | the format of the extension before CloudEvents, still read |

A chunked body (or the `lines=true` option) is read line by line, each line one event. What a request
does not say is filled in: a fresh `id`, `/otoroshi/routes/<route id>` as `source` (or the `source`
option), `com.cloud-apim.event-brokers.event` as `type` (or the `type` option), now as `time`.

## What it answers

One JSON line per event, in order, with the id of the event, once the broker has taken it or
refused it:

```json
{"published": true, "id": "4f1c...", "details": {"topic": "orders", "partition": 0, "offset": 42}}
{"published": false, "id": "77ab...", "error": "publish_error", "error_description": "no queue got the message: ..."}
{"published": false, "error": "invalid_event", "error_description": "source is required"}
```

A broker refusing one event does not fail the others. What the details say:

| Broker | Details |
|---|---|
| Kafka | `topic`, `partition`, `offset` |
| Pulsar | `topic`, `message_id` |
| RabbitMQ | the broker's confirm; a message no queue gets is a failed result, not dropped in silence |
| Redis pub/sub | `channel`, and how many subscribers got the message (`receivers`) |
| Redis streams | `stream`, the `id` of the entry |
| MQTT | `topic` |
| Artemis | `address`, `routing_type`, its JMS `message_id` |

A body that is not valid at all is refused with a `400`.

## Options

| Option | |
|---|---|
| `topic` | where to (`queue`, `exchange` and `routing_key` on RabbitMQ, `channel` or `stream` on Redis, `address` on Artemis) |
| `key`, `partition` | Kafka (an event's `partitionkey` extension is its key on every broker with keys) |
| `qos`, `retained` | MQTT |
| `routing_type`, `key`, `priority`, `ttl`, `delay`, `deliver_at`, `durable` | Artemis |
| `source`, `type`, `subject` | the attributes the request does not give |
| `lines` | read the body line by line |

Events are published in batches of up to 100 (or what came within 10 ms), in order.
