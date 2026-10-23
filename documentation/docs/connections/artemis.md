---
title: ActiveMQ Artemis
description: "Artemis on its core protocol: addresses, routing types, queues, selectors, message groups."
---

# ActiveMQ Artemis

```json
{
  "url": "tcp://artemis:61616",
  "username": null,
  "password": null,
  "default_address": null,
  "default_routing_type": "multicast",
  "durable": true,
  "duplicate_detection": false,
  "consumer_window_size": 1048576,
  "reconnect_attempts": -1,
  "call_timeout_ms": 10000,
  "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true }
}
```

`url` is required: an Artemis core url, a list of brokers (`(tcp://a:61616,tcp://b:61616)?ha=true`),
with its netty parameters; `tls` adds the TLS ones (`sslEnabled`, the key stores).

- `durable`: messages written to disk by the broker, kept across a restart (the `durable` option per
  call).
- `duplicate_detection` gives each message the source and the id of its event as duplicate id
  (`_AMQ_DUPL_ID`): the broker drops an event published twice.
- `consumer_window_size`: what a consumer buffers ahead, in bytes (`0` none, `-1` no limit).
- The client reconnects by itself `reconnect_attempts` times (`-1`: forever); while it does, a call
  waits `call_timeout_ms` at most.

## Addresses and queues

The extension speaks the core protocol, not JMS: it keeps Artemis' own model.

- What is sent to a **multicast** address goes to every queue bound to it. A subscription without a
  `group` gets a temporary queue of its own (every message); with a `group`, the members share a
  durable queue named after the group, which keeps what is published while none of them is there.
  That is what a JMS shared durable subscription is: a JMS application subscribing under the same
  name shares it too.
- What is sent to an **anycast** address goes to its queue, named after it, which its consumers
  share. A `group` on it is refused.
- `routing_type` says which one an address is (`queue` instead of `address` means anycast); without
  it, the connection's `default_routing_type`. `address::queue` reads, or sends to, that very queue.
- Addresses and queues are made as the broker's address settings allow (`auto-create-*`); otherwise
  the broker's answer is a `502`.

## Publishing

Each send waits for the broker, so every result says what happened to its message (an address that
does not exist, a security refusal), and gives its JMS message id. Options: `priority` (0 to 9), `ttl`
(millis), `delay` or `deliver_at` (epoch millis, a scheduled delivery), `durable`, `key`: the group id
of the message, whose messages go to one consumer at a time, in order.

## Consuming

- `selector`: an Artemis filter, which the `cloudEvents_*` properties make useful
  (`cloudEvents_type = 'order.created' AND priority > 4`).
- `prefetch` (100). `at-most-once` acks as the message leaves the subscription, `at-least-once` once
  it is handed over (on a multicast address, with a group: a temporary queue goes with its
  subscription).
- A consumer that closes gives back what it did not ack as never delivered: only a consumer that
  fails counts its messages as delivered (`artemisdeliverycount`, and `max-delivery-attempts` before
  the dead letters). What never reached a client does not get closer to the dead letters.
- A slow subscriber holds its own delivery thread, then the consumer window stops the broker: never a
  thread of a shared pool.

JMS applications read what the gateway publishes, and the other way around: the attributes go in
`cloudEvents_*` properties (see [CloudEvents](../cloudevents.md)), a JMS text message is read as a
`text/plain` event.
