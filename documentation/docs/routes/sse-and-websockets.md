---
title: SSE and websockets
description: Server-sent events that resume where the client stopped, and websockets with client acks.
---

# SSE and websockets

## Server-sent events

The **SSE events consumer** takes the [options](./consuming.md#options) of the HTTP consumer, and:

- names each event after its `type` (`event:`), and gives every line of the data its own `data:` field
- sends a `: keep-alive` comment every 15 seconds without events (`heartbeat=<millis>`, `0` to stop)
- gives each event an `id:`
- answers with `Cache-Control: no-cache` and `X-Accel-Buffering: no`, so that no proxy holds the events

```text
id: kafka:eyJvcmRlcnM6MCI6IjQyIn0
event: com.acme.order.created
data: {"specversion":"1.0","id":"...","source":"/shop","type":"com.acme.order.created","data":{"order":42}}
```

### Resuming

On Kafka, Redis streams, Pulsar and RabbitMQ streams, the `id` of an event holds the positions read so
far, on every partition, stream or topic. A client coming back with it in `Last-Event-ID` (what a
browser's `EventSource` does by itself) resumes **right after what it got**:

```js
const events = new EventSource('https://events.example.com/?x-ca-event-broker-opts-from=earliest');
events.addEventListener('com.acme.order.created', (e) => console.log(JSON.parse(e.data)));
```

A partition the client got nothing from yet starts where the call asks to start (`from`): the same
URL, again, as `EventSource` does. In a group (Kafka, Redis streams), the group knows where it is: the
position is not used. Elsewhere, the `id` is the id of the event.

## Websockets

The **Websocket Ingester/Consumer** has an `in` and an `out` configuration, each one a plugin
configuration (either can be left without a `ref`):

```json
{
  "in": { "ref": "connection_...", "options": { "topic": "commands" } },
  "out": { "ref": "connection_...", "options": { "topic": "updates", "group": "ui" } }
}
```

- Frames sent by the client are published, one at a time. A text frame holding a structured
  CloudEvent is that event; anything else is the data of a new one. With `publish_results=true` (on
  `in`), every frame published is answered with its result.
- Events consumed are sent as frames, in the `format` asked for.
- With `acks=client` (on `out`), an event is acked only when the client answers `{"ack": "<event id>"}`
  (or a list of ids), and no more than `max_in_flight` (100) wait for their ack. With
  `delivery=at-least-once`, what the client did not ack is delivered again.

Each side follows the policy of its own configuration: a caller option is refused only if no side
that takes it allows it.
