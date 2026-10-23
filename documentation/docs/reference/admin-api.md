---
title: Admin API
description: The entities of the extension and its admin routes.
---

# Admin API

Every call takes the credentials of the admin API (`-u admin-api-apikey-id:admin-api-apikey-secret`
on a fresh Otoroshi), on its host (`otoroshi-api.oto.tools`).

## Entities

| Entity | Path |
|---|---|
| Connections | `/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections` |
| Webhooks | `/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-webhooks` |

The usual routes of Otoroshi's entities: `GET` (all), `GET /<id>`, `POST`, `PUT /<id>`, `PATCH /<id>`,
`DELETE /<id>`, `_template` (`_template?kind=<kind>` for a connection), bulk routes. A write whose
settings are not valid is a `400` listing every field at fault.

## Routes

| Method | Path | |
|---|---|---|
| `GET` | `/api/extensions/cloud-apim/event-brokers/connections/_status` | the status of every connection on this node, with its traffic |
| `GET` | `/api/extensions/cloud-apim/event-brokers/connections/<id>/_status` | the status of a connection |
| `POST` | `/api/extensions/cloud-apim/event-brokers/connections/<id>/_publish` | publishes the body ([Try a connection](../features/try-a-connection.md)) |
| `GET` | `/api/extensions/cloud-apim/event-brokers/connections/<id>/_peek` | reads a few events without acking them |
| `GET` | `/api/extensions/cloud-apim/event-brokers/connections/<id>/_lag?group=<group>&topic=<topic>` | how far behind a group is ([Observability](../operations/observability.md)) |
| `GET` | `/api/extensions/cloud-apim/event-brokers/_traffic` | the traffic of the connections and the routes of this node |
| `GET` | `/api/extensions/cloud-apim/event-brokers/webhooks/_status` | the webhooks of this node, and who delivers them |
| `GET` | `/api/extensions/cloud-apim/event-brokers/webhooks/<id>/_status` | a webhook |

They answer for the node serving them: every node has its own clients.
