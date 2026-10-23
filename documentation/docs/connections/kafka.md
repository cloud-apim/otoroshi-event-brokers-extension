---
title: Kafka
description: The settings of a Kafka connection, and what Kafka brings.
---

# Kafka

```json
{
  "bootstrap_servers": ["kafka-1:9092", "kafka-2:9092"],
  "default_topic": null,
  "security_protocol": "PLAINTEXT",
  "sasl": { "mechanism": "PLAIN", "username": null, "password": null, "jaas_config": null },
  "tls": {
    "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true,
    "keystore_path": null, "truststore_path": null, "store_password": null
  },
  "producer": {
    "acks": null, "idempotence": null, "compression": null, "linger_ms": null, "batch_size": null,
    "max_in_flight": null, "delivery_timeout_ms": null
  },
  "properties": {}
}
```

`bootstrap_servers` is required.

## Security

`security_protocol` is `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`.

- The `SASL_*` protocols need a `username` and a `password`, with the login module of the `mechanism`
  (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`), or a whole `jaas_config` (`OAUTHBEARER`, `GSSAPI`).
- `SSL` and `SASL_SSL` use the certificates of the Otoroshi store (`tls.client_certs`,
  `tls.trusted_certs`, given to Kafka as inline PEM, no file), or key stores in files (`keystore_path`,
  `truststore_path`, `store_password`), not both. Kafka has no way to trust any certificate:
  `trust_all` is refused, give the certificate of its CA instead. See [TLS and authentication](./tls-and-auth.md).

## The producer

`producer` tunes how events are sent: `acks` (`all`, `1`, `0`), `idempotence` (which needs `acks`
`all` and 5 requests in flight at most), `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`),
`linger_ms`, `batch_size`, `max_in_flight`, `delivery_timeout_ms`. What is not set keeps the default
of the Kafka client. `properties` are raw Kafka client properties, applied last: anything else goes
there.

## What Kafka brings

- **Publishing**: the result of each event gives its `topic`, `partition` and `offset`. `key` and
  `partition` options; an event's `partitionkey` extension is its record key.
- **Consuming**: a consumer group of its own per subscription without a `group` (every event), the
  named consumer group with one. `at-least-once` commits what was handed over.
- **Replay**: `from=earliest`, `from_timestamp`, `offset_from` (`partition` for one partition,
  `offset_to` to stop).
- **Server-sent events resume**: the id of each event holds the offsets read on every partition.
- **Lag**: how far behind a group is, from its committed offsets (see [Observability](../operations/observability.md)).
- Options `client_id`, `group_instance_id` (static membership).
