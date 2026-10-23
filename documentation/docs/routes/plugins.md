---
title: The plugins
description: The plugins that put a connection behind a route, and what a route lets its callers ask.
---

# The plugins

| Plugin | Does |
|---|---|
| **Cloud APIM - HTTP events ingester** | publishes the request body: one event, a batch, or newline-delimited events ([Publishing](./publishing.md)) |
| **Cloud APIM - HTTP events consumer** | streams events as newline-delimited JSON ([Consuming](./consuming.md)) |
| **Cloud APIM - SSE events consumer** | streams events as server-sent events ([SSE and websockets](./sse-and-websockets.md)) |
| **Cloud APIM - Websocket Ingester/Consumer** | publishes incoming frames, streams events back ([SSE and websockets](./sse-and-websockets.md)) |
| **Cloud APIM - Events request reply** | publishes the request, answers with the event replying to it ([Request reply](./request-reply.md)) |

Their identifiers are in the [reference](../reference/plugins.md). They call no backend: the target of
their route does not matter.

## Configuration

Every plugin points to a connection, and decides what its callers may ask:

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

- `options` are what the route fixes: the [options](../reference/options.md) of a publish or a
  subscription, the topic first. Their values may hold Otoroshi
  [expressions](https://www.otoroshi.io/docs/topics/expression-language) (`${apikey.metadata.tenant}`,
  `${req.headers.x-tenant}`, `${user.email}`), evaluated for each call.
- A caller sets options with query parameters and headers prefixed with `x-ca-event-broker-opts-`
  (`?x-ca-event-broker-opts-take=10`), but **only those in `allowed_overrides`**: anything else is
  refused with a `400`. By default a caller can only shape its own response (`take`, `drop`,
  `duration`, `format`, `heartbeat`); the topic, the group, the start position, the delivery stay the
  route's. `["*"]` allows everything.
- What a caller sends is never evaluated as an expression.
- When the caller may name the topic (queue, channel, stream, address), `allowed_topics` holds the glob
  patterns it must match (`["orders-*"]`); empty, any.
- Where an event says it goes (the `vendor_metadata` of the legacy format) is held to the same rules.
- `include_query` and `include_headers`: where the options of a caller are read from.

The route's own security (api keys, JWT verification, authentication modules, quotas) applies
before: the plugin only decides what an allowed caller may ask of the broker.

## Errors

A plugin answers an error before streaming anything when it cannot serve the call:

| Status | When |
|---|---|
| `400` | what the call got wrong: an option it may not set, no topic, something the broker cannot do |
| `500` | the route points to a connection that does not exist |
| `502` | what the broker refused: a queue that does not exist, a subscription it would not open |
| `503` | the connection is not connected on this node, with a `Retry-After` |
