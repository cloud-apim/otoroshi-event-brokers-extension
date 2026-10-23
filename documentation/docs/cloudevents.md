---
title: CloudEvents
description: Every event is a CloudEvent, with the protocol binding of each broker.
---

# CloudEvents

Every event going in or out is a [CloudEvents 1.0](https://cloudevents.io/) event, whatever the broker.
What the gateway publishes, a consumer outside Otoroshi (a Kafka consumer, a JMS application, a
CloudEvents SDK) reads; and what another producer published is read as an event too.

## On each broker

The CloudEvents protocol bindings:

| Broker | Mode | Where the attributes are |
|---|---|---|
| Kafka | binary | `ce_*` headers, the data as the value, `content-type` header, `partitionkey` as the record key |
| Pulsar | binary | `ce_*` properties, the data as the payload, `partitionkey` as the message key |
| RabbitMQ | binary | `cloudEvents:*` application properties, the AMQP `content-type` |
| MQTT 5 | binary | user properties (no prefix), the MQTT 5 content type |
| Artemis | binary | `cloudEvents_*` properties, `cloudEvents_datacontenttype` included (the JMS flavour of the AMQP binding: JMS clients and selectors read them), the data as the body of a bytes message, `partitionkey` as the group id |
| Redis streams | binary | `ce_*` fields of the entry, `content-type` field, the data in the `data` field |
| Redis pub/sub, MQTT 3.1.1 | structured | no headers: the whole event as JSON in the payload |

## Messages without attributes

A message another producer published without CloudEvents attributes is read as an event: its `id`
is its position when the broker has one (`<topic>-<partition>-<offset>` on Kafka, `<stream>-<entry id>`
on Redis streams, the message id elsewhere), its `source` `/<kind>/<topic>`, its `type`
`com.cloud-apim.event-brokers.message`, its time the broker's, its content type guessed from the
payload when nothing says it (JSON, text, or bytes).

## What the broker says

Every event read carries what the broker says of the message, as extensions named after the kind:
`kafkatopic`, `kafkapartition`, `kafkaoffset`, `kafkatimestamp`, `rabbitmqroutingkey`,
`rabbitmqredelivered`, `rabbitmqstreamoffset`, `pulsartopic`, `pulsarmessageid`, `redischannel`,
`redisstream`, `redisid`, `redisredelivered`, `mqtttopic`, `mqttqos`, `artemisaddress`, `artemisqueue`,
`artemisdeliverycount`, `artemisgroupid`, ...

## What consumers give

`format=cloudevents` (the default): the structured events, as JSON. `format=raw`: their data alone.
