---
title: Events
description: The Otoroshi events and alerts of the extension.
---

# Events

## EventBrokerConnectionEvent

Sent each time a connection changes state on a node.

```json
{
  "@id": "...", "@timestamp": 1792663171364, "@type": "EventBrokerConnectionEvent",
  "@product": "otoroshi", "@serviceId": "connection_...", "@service": "orders", "@env": "prod",
  "connection": { "id": "connection_...", "name": "orders", "broker": "kafka" },
  "previous_state": "connecting",
  "state": "connected",
  "status": { "state": "connected", "since": 1792663171364, "error": null, "failures": 0 }
}
```

## EventBrokerConnectionUnavailableAlert

An `AlertEvent`, `alert: EventBrokerConnectionUnavailable`: a connection could not connect, or lost
its broker. One per outage.

```json
{
  "@type": "AlertEvent", "alert": "EventBrokerConnectionUnavailable",
  "connection": { "id": "connection_...", "name": "orders", "broker": "kafka" },
  "previous_state": "connected",
  "error": "TimeoutException: ...",
  "status": { "state": "unavailable", "failures": 2 }
}
```

`previous_state` says which: `connecting` (it never worked with these settings) or `connected` (it
lost its broker).

## EventBrokerWebhookDeliveryFailedAlert

An `AlertEvent`, `alert: EventBrokerWebhookDeliveryFailed`: a webhook gave an event up (refused by its
endpoint, or every attempt failed), and no dead letters took it.

```json
{
  "@type": "AlertEvent", "alert": "EventBrokerWebhookDeliveryFailed",
  "webhook": { "id": "webhook_...", "name": "orders to billing", "connection": "connection_...", "url": "https://billing.internal/events" },
  "event": { "id": "...", "source": "/shop", "type": "com.acme.order.created" },
  "attempts": 5,
  "reason": "the endpoint answered 503"
}
```
