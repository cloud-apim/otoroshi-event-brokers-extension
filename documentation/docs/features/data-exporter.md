---
title: Data exporter
description: Otoroshi's own events (analytics, alerts, audit) published to an event broker.
---

# Data exporter

Otoroshi sends its events (the analytics of every call, alerts, audit, the events of this extension)
to **data exporters**. The *Event brokers exporter* is one: its events go to a connection.

In **Data exporters**, add a `custom` exporter, choose *Cloud APIM - Event brokers exporter*, and set
its configuration:

```json
{ "connection": "connection_...", "options": { "topic": "otoroshi-events" }, "format": "cloudevents" }
```

The filtering, the projection and the buffering of the exporter are Otoroshi's own settings.

## The events

Each event is a CloudEvent:

| Attribute | Is |
|---|---|
| `id` | its `@id` |
| `type` | `io.otoroshi.<@type>`: `io.otoroshi.GatewayEvent`, `io.otoroshi.AlertEvent`, ... |
| `source` | `/<@product>/<@env>` |
| `subject` | the alert, or the route |
| `time` | its `@timestamp` |
| `partitionkey` | the route: the events of a route stay in order on Kafka |
| data | the event, as JSON |

`format: raw` publishes the event alone, without any attribute: what a consumer expecting Otoroshi's
JSON gets.

A batch the broker refuses fails the export: Otoroshi's exporter decides what to do then.
