---
title: Try a connection
description: Publish a test event, or peek at what a topic or a queue holds, from the backoffice or the admin API.
---

# Try a connection

The page of a connection in the backoffice has a **Try it** section, once the connection is saved: the
options of a call (as JSON, `{}` for the default topic of the connection), an event, and two buttons,
**Publish** and **Peek**. The same is in the admin API.

## Publish

```bash
curl -X POST 'http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/<id>/_publish?topic=orders' \
  -u admin-api-apikey-id:admin-api-apikey-secret -H 'Content-Type: application/json' -d '{"hello":"world"}'
```

```json
{ "results": [{ "published": true, "id": "...", "details": { "topic": "orders", "partition": 0, "offset": 7 } }] }
```

The query string holds the [options](../reference/options.md) of the publish. A body holding a
CloudEvent (with a `specversion`), or an array of them, is published as it is; anything else is the
data of a new event.

## Peek

```bash
curl 'http://otoroshi-api.oto.tools:8080/api/extensions/cloud-apim/event-brokers/connections/<id>/_peek?topic=orders&from=earliest&take=10&within=3000' -u ...
```

```json
{ "events": [ { "specversion": "1.0", "id": "...", "data": { "hello": "world" } } ], "acked": false }
```

At most `take` events (10), within `within` millis (5000). **A peek never acks**: what it reads from a
queue (RabbitMQ, an anycast address of Artemis) or in a group goes back where it was. `acked` says
whether what was read was taken away (on brokers without acks, like Redis pub/sub, a peek only sees
what is published while it is there).

Both answer for the node serving the admin API, with its client of the connection.
