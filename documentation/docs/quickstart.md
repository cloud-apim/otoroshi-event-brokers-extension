---
sidebar_position: 2
title: Quickstart
description: A broker, a connection, two routes, an event.
---

# Quickstart

A Kafka broker, an Otoroshi with the extension, a connection, a route to publish and a route to
consume.

## 1. A broker

The repository's `docker-compose.yml` runs every broker on its default port. Kafka alone is enough
here:

```bash
docker compose up -d kafka
```

Kafka listens on `localhost:9093`.

## 2. Otoroshi and the extension

Download Otoroshi 18 and the extension's jar from the
[releases page](https://github.com/cloud-apim/otoroshi-event-brokers-extension/releases/latest), then
start them together:

```bash
java -cp "./otoroshi-event-brokers-extension_3-<version>.jar:./otoroshi.jar" play.core.server.ProdServerStart
```

The backoffice is on `http://otoroshi.oto.tools:8080` (the first start prints the credentials of its
admin in the logs), the admin API on `http://otoroshi-api.oto.tools:8080`.

## 3. A connection

In the backoffice, **Event broker connections** → **Add item**, kind `kafka`, bootstrap servers
`localhost:9093`. Or through the admin API:

```bash
curl -X POST http://otoroshi-api.oto.tools:8080/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections \
  -u admin-api-apikey-id:admin-api-apikey-secret -H 'Content-Type: application/json' -d '{
    "id": "connection_kafka", "name": "kafka", "description": "", "tags": [], "metadata": {},
    "broker": "kafka", "settings": { "bootstrap_servers": ["localhost:9093"], "default_topic": "orders" }
  }'
```

Its status, a few seconds later, says `connected`:

```bash
curl http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/connection_kafka/_status \
  -u admin-api-apikey-id:admin-api-apikey-secret
```

## 4. Two routes

Create a route on `events.oto.tools` with the **Cloud APIM - HTTP events ingester** plugin, and a
route on `events-stream.oto.tools` with the **Cloud APIM - SSE events consumer** plugin, both with
this configuration:

```json
{ "ref": "connection_kafka", "options": { "topic": "orders" } }
```

The plugins call no backend: the target of the routes does not matter.

## 5. Publish, consume

In one terminal, consume:

```bash
curl -N http://events-stream.oto.tools:8080/
```

In another, publish:

```bash
curl -X POST http://events.oto.tools:8080/ -H 'Content-Type: application/json' -d '{"order": 42}'
```

The ingester answers one line per event, once Kafka took it:

```json
{"published": true, "id": "4f1c...", "details": {"topic": "orders", "partition": 0, "offset": 0}}
```

and the consumer gets it as a server-sent event, a [CloudEvent](./cloudevents.md):

```text
id: kafka:eyJvcmRlcnM6MCI6IjAifQ
event: com.cloud-apim.event-brokers.event
data: {"specversion":"1.0","id":"4f1c...","source":"/otoroshi/routes/route_...","type":"com.cloud-apim.event-brokers.event","datacontenttype":"application/json","time":"...","kafkatopic":"orders","kafkapartition":"0","kafkaoffset":"0","data":{"order":42}}
```

## Next

- [Publishing](./routes/publishing.md) and [Consuming](./routes/consuming.md): what the plugins take
- [The plugins](./routes/plugins.md): what a route lets its callers ask
- [Webhooks](./features/webhooks.md): the same events, pushed to an HTTP endpoint
