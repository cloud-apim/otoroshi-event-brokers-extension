---
title: Troubleshooting
description: What the errors say, and where to look.
---

# Troubleshooting

## A route answers 503

The connection is not connected on the node that served the call. Its status says why:

```bash
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/<id>/_status -u ...
```

- `connecting` for a while: the broker does not answer, or the address is wrong. `error` gives the
  innermost message of the client (`Connection refused`, `timed out`).
- `unavailable` with `invalid settings: ...`: the settings stored do not fit the kind (written by hand,
  or by an older version). Save them again from the backoffice or the admin API, which checks them.
- `unavailable` with a security message: wrong credentials (`WRONGPASS`, `ACCESS_REFUSED`,
  `Authentication failed`, `AMQ229031`), a certificate not trusted (`PKIX path building failed`), a
  name the certificate does not hold (`No subject alternative names`), a certificate not in the store
  (`the client certificate '...' is not in the certificate store (yet?)`).
- `pending` set: new settings do not connect, the previous ones still serve.

## A route answers 400

The call asked for something it may not, or the broker cannot do: the `error_description` says which
(`the caller cannot set 'topic'`, `Redis has no consumer groups`, `only a rabbitmq stream replays`).
What a caller may ask is the route's [configuration](../routes/plugins.md).

## A route answers 500

The plugin points to a connection that does not exist (`connection_not_found`).

## A consumer gets nothing

- On pub/sub brokers (Redis pub/sub, MQTT, Pulsar from `latest`), what was published before the
  subscription is gone: see [Consuming](../routes/consuming.md#pubsub-brokers).
- In a group, the other members may have the events: a group shares them.
- On Kafka, a new group starts from `latest` unless `from=earliest`.

## A webhook does not deliver

Its [status](../features/webhooks.md#status): `owner` empty and an error in a cluster means the shared
Redis is missing (`webhooks.redis.uri`). `last_error` gives the last failure of a delivery; an
`EventBrokerWebhookDeliveryFailed` alert is sent for every event given up on without dead letters.

## Logs

The loggers of the extension: `cloud-apim-event-brokers-extension`,
`cloud-apim-event-brokers-connections`, `cloud-apim-event-brokers-plugins`,
`cloud-apim-event-brokers-webhooks`, `cloud-apim-event-brokers-replies`.
