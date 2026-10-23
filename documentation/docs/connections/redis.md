---
title: Redis
description: Redis pub/sub, or Redis streams with consumer groups, acks and replay.
---

# Redis

```json
{
  "uri": "redis://redis:6379",
  "username": null,
  "password": null,
  "mode": "pubsub",
  "default_channel": null,
  "streams": {
    "default_stream": null,
    "max_length": null,
    "exact_trimming": false,
    "data_field": "data",
    "claim_min_idle_ms": 60000
  },
  "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true, "start_tls": false },
  "io_thread_pool_size": null,
  "computation_thread_pool_size": null
}
```

`uri` is required: `redis://`, or `rediss://` for TLS (which `tls.enabled` turns on too). The
`username` and `password` are those of an ACL user (the password alone for the `default` user).

`mode` is `pubsub` or `streams`.

## Pub/sub

Channels: what is published reaches the subscribers there are, and no one else. A publish says how
many subscribers got the message. Names with glob characters (`*`, `?`, `[`) are patterns. Pub/sub has
no acks, no groups and no replay; a subscriber too slow loses the oldest messages.

## Streams

An append-only log, with consumer groups and acks.

- **Publishing**: `XADD`, the attributes of the event in `ce_` fields next to the `data_field`.
  `max_length` trims the stream on each publish, to about that many entries (`MAXLEN ~`), exactly with
  `exact_trimming` (`0` or `null`: no limit). The result gives the id of the entry.
- **Without a group**: `XREAD` from now on (the last entry when subscribing: nothing slips between two
  reads), from the beginning (`from=earliest`), a date (`from_timestamp`) or an entry id (`from_id`,
  included). Every subscriber gets every entry. `at-least-once` needs a group.
- **With a group**: `XREADGROUP`. The group is created where `from` says if it does not exist, and
  keeps its own position otherwise. An entry read by a member is its own until acked: as it leaves the
  subscription (`at-most-once`) or once handed over (`at-least-once`). What a member read and left
  unacked for `claim_min_idle_ms` (it went away, it was cut) is claimed by another member
  (`XAUTOCLAIM`) and delivered again, with `redisredelivered`.
- Each subscription of a group is a consumer of its own, removed from the group when it goes away with
  nothing pending. `consumer=<name>` names it instead: a named consumer coming back gets what it had
  pending first (one subscription per name).
- `prefetch` (100): what a read asks for. A smaller prefetch shares the entries more evenly between
  slow members.
- An entry added by another producer without a `data_field` is read with all its fields as a JSON
  object (`XADD orders * user 42` gives `{"user": "42"}`).
- **Server-sent events resume** right after the last entry a client got, stream by stream.
- **Lag**: what `XINFO GROUPS` says of a group (see [Observability](../operations/observability.md)).
