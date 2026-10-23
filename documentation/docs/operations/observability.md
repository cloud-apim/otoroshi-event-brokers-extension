---
title: Observability
description: Events, alerts, traffic counters, the lag of the groups, and metrics.
---

# Observability

## Events and alerts

| Event | `@type` / `alert` | Sent when |
|---|---|---|
| `EventBrokerConnectionEvent` | `EventBrokerConnectionEvent` | a connection changes state on a node |
| `EventBrokerConnectionUnavailableAlert` | `AlertEvent` / `EventBrokerConnectionUnavailable` | a connection cannot connect, or loses its broker |
| `EventBrokerWebhookDeliveryFailedAlert` | `AlertEvent` / `EventBrokerWebhookDeliveryFailed` | a webhook gives an event up, and no dead letters took it |

They go through Otoroshi's data exporters, like every other event (to a broker too, with the
[exporter](../features/data-exporter.md)). Their content is in the [reference](../reference/events.md).
One unavailability alert per outage, not one per attempt.

## Traffic

Every node counts what goes through its connections and its routes since it started: in the status
of each connection, in the backoffice list, and in the admin API.

```bash
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/_traffic -u ...
```

```json
{
  "connections": { "connection_...": { "published": 1204, "publish_failed": 2, "consumed": 980, "acked": 980, "subscriptions": 3, "publish_ms": { "mean": 4.2, "max": 87 } } },
  "routes": { "route_...": { "published": 1204, "publish_failed": 2, "consumed": 0, "acked": 0, "subscriptions": 0, "publish_ms": { "mean": 4.2, "max": 87 } } }
}
```

`subscriptions` are the ones open now; `publish_ms` is how long the broker took to take a batch of
events. Webhooks, the exporter, the workflow functions and request reply count on their connection.

## How far behind a group is

Where the broker tells it: Kafka, from the committed offsets of the group against the end of each
partition; Redis streams, from `XINFO GROUPS` (its `lag`, what is pending, the last entry delivered).
Elsewhere, a `501`.

```bash
curl 'http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/<id>/_lag?group=billing&topic=orders' -u ...
```

```json
{ "group": "billing", "lag": 3, "partitions": [{ "topic": "orders", "partition": 0, "committed": 2, "end": 5, "lag": 3 }] }
```

A webhook reads in the group `otoroshi-webhook-<id>`: its lag says how far behind its deliveries are.

## Metrics

When Otoroshi's metrics are enabled (its Prometheus export included):

| Metric | |
|---|---|
| `cloud-apim.event-brokers.connections.<state>` | how many connections of the node are in each state |
| `cloud-apim.event-brokers.connections.transitions.<state>` | how many times a connection went into a state |
| `cloud-apim.event-brokers.connections.<id>.published`, `.publish_failed`, `.consumed`, `.acked` | the traffic of a connection |
| `cloud-apim.event-brokers.connections.<id>.publish` (timer), `.subscriptions` (gauge) | how long publishing takes, what is open |
| `cloud-apim.event-brokers.routes.<id>.*` | the same, for a route |
| `cloud-apim.event-brokers.webhooks.<id>.delivered`, `.retried`, `.failed`, `.dead_lettered` | the deliveries of a webhook |
