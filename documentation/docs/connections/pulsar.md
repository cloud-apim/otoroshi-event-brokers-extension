---
title: Pulsar
description: The settings of a Pulsar connection, and what Pulsar brings.
---

# Pulsar

```json
{
  "service_url": "pulsar://pulsar:6650",
  "tenant": "public",
  "namespace": "default",
  "default_topic": null,
  "auth": { "kind": "none" },
  "tls": {
    "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true,
    "trust_certs_file_path": null
  }
}
```

`service_url` is required: `pulsar://`, or `pulsar+ssl://` with `tls.enabled`.

- `auth` is `{"kind": "none"}`, `{"kind": "token", "token": "..."}` (a JWT, as Pulsar's token
  authentication takes it) or `{"kind": "basic", "username": "...", "password": "..."}`.
- Topics without a scheme are `persistent://<tenant>/<namespace>/<topic>`.
- `trust_certs_file_path`: a PEM file of trusted certificates on the node, next to the ones of the
  store.

## What Pulsar brings

- **Producers** are kept, one per topic (the 64 most recently used). The result of each event gives
  its message id. An event's `partitionkey` is its message key.
- **Consuming**: without a `group`, a non durable subscription of its own (every message, nothing left
  on the broker). With a `group`, a durable subscription, shared by default (`subscription_type`:
  `exclusive`, `shared`, `failover`, `key_shared`): it keeps what is published while none of its
  subscribers is connected.
- `at-least-once` acks once handed over; what was not acked is delivered again.
- **Replay**: `from=earliest` (for a new subscription), `from_timestamp`.
- **Server-sent events resume** by message id, topic by topic (partition by partition).
