---
title: FAQ
description: Questions that come back.
---

# FAQ

## Do my clients need a broker client library?

No: an HTTP client is enough to publish, `curl -N` or a browser's `EventSource` to consume. The
events are CloudEvents, so the consumers that do read the broker directly (a Kafka consumer, a JMS
application) read the same events.

## Is an event delivered exactly once?

No broker behind HTTP can promise that. `at-most-once` (the default) never delivers an event twice
and may lose what a client did not get; `at-least-once` never loses what was not handed over and may
deliver it twice. Give your events stable ids (the CloudEvents `id`): consumers can drop what they
saw. On Artemis, `duplicate_detection` drops an event published twice.

## What happens when a broker goes away?

Its connection becomes `unavailable` on each node: the routes answer `503` with a `Retry-After`,
the open streams end, an alert is sent once. The client tries again in the background and the
connection comes back by itself; the clients coming back resume where they stopped (server-sent
events, groups).

## Can a caller read any topic?

Not unless the route says so. By default a caller can only shape its own response; the topic is the
route's. With `allowed_overrides: ["topic"]`, `allowed_topics` restricts the topics to glob patterns.
The route's own security applies first.

## Does it work in a cluster?

Yes: every node has its own clients, and serves the calls that reach it. Webhooks need a Redis the
nodes share, to be delivered by one node at a time: see [Running in a cluster](./operations/cluster.md).

## Which Otoroshi version?

Otoroshi 18 and later, on Java 17 and later.

## Where are the release notes?

On the [releases page](https://github.com/cloud-apim/otoroshi-event-brokers-extension/releases).
What is planned and done is in the repository's `ROADMAP.md`.
