---
title: Webhooks
description: Push the events of a connection to HTTP endpoints, retried, signed, dead-lettered, one node at a time.
---

# Webhooks

A webhook pushes the events of a connection to an HTTP endpoint: a subscription (the
[options](../routes/consuming.md#options) a consumer route takes), an endpoint, a retry policy and,
optionally, dead letters. Webhooks have their page in the backoffice (**Event broker webhooks**).

| Entity | Kind | Admin API |
|---|---|---|
| Event broker webhook | `event-brokers.extensions.cloud-apim.com/EventBrokerWebhook` | `/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-webhooks` |

```json
{
  "id": "webhook_...", "name": "orders to billing", "enabled": true,
  "connection": "connection_...",
  "subscription": { "topic": "orders", "from": "earliest" },
  "endpoint": {
    "url": "https://billing.internal/events", "method": "POST", "headers": {}, "timeout_ms": 10000,
    "mode": "structured", "secret": "${vault://local/billing/webhook-secret}",
    "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true }
  },
  "retry": { "max_attempts": 5, "initial_backoff_ms": 1000, "max_backoff_ms": 30000 },
  "max_in_flight": 1,
  "dead_letter": { "connection": null, "options": { "topic": "orders-dlq" } }
}
```

## The requests

- `mode` is how an event goes over HTTP (the CloudEvents HTTP binding): `structured` (the event as
  `application/cloudevents+json`), `binary` (the attributes in `ce-` headers, the data as the body) or
  `raw` (the data alone, with its content type).
- Every request carries `X-Event-Brokers-Webhook` (the id of the webhook) and
  `X-Event-Brokers-Attempt` (from 1), and the `headers` of the endpoint.
- With a `secret`, every body is signed: `X-Event-Brokers-Signature: sha256=<hex of the hmac-sha256 of the body>`.
  An endpoint checks it before trusting the event:

  ```js
  const expected = 'sha256=' + crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
  ```

- `tls` reaches the endpoint with certificates of the Otoroshi store (mutual TLS, a private CA).

## Delivery

- A `2xx` delivers the event, and only then is it acked: what was not delivered is delivered again.
- A `408`, `429`, `5xx` or no answer is tried again, waiting twice as long each time from
  `initial_backoff_ms` up to `max_backoff_ms` (and at least what a `Retry-After` asks),
  `max_attempts` times in all. Any other `4xx` is a refusal, not tried again.
- An event given up on goes to the `dead_letter` connection (the webhook's own one by default), with
  the options of the publish there, and three extensions: `webhookid`, `webhookerror`,
  `webhookattempts`. Without dead letters, an `EventBrokerWebhookDeliveryFailed`
  [alert](../reference/events.md) is sent. Either way, it is acked: one event does not hold the
  others forever.
- `max_in_flight` deliveries at once: `1` keeps the order of the events.
- Unless the subscription says otherwise, a webhook reads in a group of its own
  (`otoroshi-webhook-<id>`) where the broker has groups, and acks at least once where it can. A node
  that takes the webhook over goes on where the group stopped.

## One node at a time

One node at a time delivers a webhook. Alone, Otoroshi delivers them all. In a cluster, the nodes
decide in a Redis they share (`webhooks.redis.uri`, through Otoroshi's stateful clients): the one
holding the lock of a webhook delivers it, and renews its lock; once it stops (it goes away, it lost
the Redis), another one takes it over after `webhooks.lock-ttl`. A cluster without that Redis delivers
no webhook: the status says why. See [Running in a cluster](../operations/cluster.md).

## Status

```bash
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/webhooks/_status -u ...
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/webhooks/<id>/_status -u ...
```

```json
{ "id": "webhook_...", "name": "orders to billing", "delivering": true, "owner": "node-1-3f2a9c1b",
  "delivered": 1204, "retried": 3, "failed": 1, "dead_lettered": 1, "last_delivery_at": 1792663181364,
  "last_error": { "at": 1792663171364, "message": "could not deliver event '...' after 5 attempt(s): ..." } }
```

`owner` is the node holding the webhook, `delivering` whether it is the node answering. The lag of its
group says how far behind its deliveries are: `_lag?group=otoroshi-webhook-<id>` (see
[Observability](../operations/observability.md)).
