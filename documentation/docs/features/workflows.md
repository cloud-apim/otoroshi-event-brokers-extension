---
title: Workflow functions
description: Publish and consume events from Otoroshi workflows.
---

# Workflow functions

The extension adds two functions to Otoroshi
[workflows](https://maif.github.io/otoroshi/manual/docs/topics/workflows), in the *Cloud APIM - Event
brokers* category.

## Publish

`extensions.com.cloud-apim.event-brokers.publish`

| Argument | |
|---|---|
| `connection` | required: the id of the connection |
| `options` | the options of the publish: `topic`, `queue`, `key`, ... |
| `event`, `events`, or `data` | a CloudEvent, several, or the data of a new event |
| `type`, `source`, `subject` | the attributes of the new event |

```json
{
  "kind": "call",
  "function": "extensions.com.cloud-apim.event-brokers.publish",
  "args": {
    "connection": "connection_...",
    "options": { "topic": "orders" },
    "data": { "id": 42 },
    "type": "com.acme.order.created"
  }
}
```

It returns `{"results": [...], "published": true}`, one result per event.

## Consume

`extensions.com.cloud-apim.event-brokers.consume`

| Argument | |
|---|---|
| `connection` | required: the id of the connection |
| `options` | the options of the subscription: `topic`, `group`, `from`, `delivery`, ... |
| `take` | how many events at most (1) |
| `within_ms` | how long to wait for them (5000) |

It returns `{"events": [...]}`, as CloudEvents, and acks what it returns.
