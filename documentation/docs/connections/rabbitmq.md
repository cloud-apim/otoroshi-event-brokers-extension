---
title: RabbitMQ
description: The settings of a RabbitMQ connection, its topology, and streams.
---

# RabbitMQ

```json
{
  "addresses": ["rabbit:5672"],
  "virtual_host": "/",
  "username": null,
  "password": null,
  "default_queue": null,
  "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true },
  "topology": {
    "exchanges": [{ "name": "orders", "type": "topic", "durable": true, "auto_delete": false, "arguments": {} }],
    "queues": [{ "name": "billing", "type": "quorum", "durable": true, "auto_delete": false, "arguments": { "x-max-length": 10000 } }],
    "bindings": [{ "exchange": "orders", "queue": "billing", "routing_key": "order.*", "arguments": {} }]
  }
}
```

`addresses` is required. An address without a port is reached on 5671 with TLS, 5672 without.

## Topology

The `topology` (empty by default) is declared as the client starts, before anything else. What exists
already, the same way, is left as it is; a declaration the broker refuses (a queue that exists with
other arguments) keeps the connection `unavailable`, saying which. Queues are `classic`, `quorum` or
`stream` (the last two durable, and not auto deleted). `arguments` are the AMQP arguments
(`x-message-ttl`, `x-dead-letter-exchange`, `x-max-length`, ...).

## Publishing

Through one channel with publisher confirms: a publish is done once the broker has taken the message.
Messages are published `mandatory`: a message no queue gets is reported as a failed result, instead of
being dropped in silence. Options: `queue` (the default exchange, routed to that queue), or `exchange`
and `routing_key`.

## Consuming

- A `queue`: its consumers share its messages (that is what a queue is). A `group` is refused: a queue
  already is one.
- An `exchange` and a `routing_key`: a queue of its own, bound to them, gone with the subscription:
  every subscriber gets every message.
- `prefetch` (100): what the broker sends ahead. `at-most-once` acks as the message leaves the
  subscription, `at-least-once` once it is handed over; what was not acked goes back to the queue.

## Streams

A queue of the `stream` type is an append-only log. Reading it does not take its messages away, and a
subscription starts where it is told (`x-stream-offset`): `from=earliest`, an offset (`offset_from`,
each event carries its `rabbitmqstreamoffset`), a date (`from_timestamp`, to the second: a stream dates
its chunks of messages), or from now on. The server-sent events consumer resumes a stream right after
the last message a client got. A queue the topology declares as a stream is known as one; otherwise
say `stream=true`. The other queues refuse to replay.
