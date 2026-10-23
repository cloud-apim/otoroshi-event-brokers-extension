---
title: TLS and authentication
description: Mutual TLS with the certificates of the Otoroshi store, and how each broker authenticates.
---

# TLS and authentication

## TLS

TLS is set the same way on every kind, with certificates of the Otoroshi certificate store:

```json
"tls": { "enabled": true, "client_certs": ["my-client-cert"], "trusted_certs": ["my-ca"], "trust_all": false, "hostname_verification": true }
```

- `client_certs`: who the gateway is. The first one is used, with its private key: mutual TLS.
- `trusted_certs`: whom it trusts, a CA or the certificates of the brokers. Without them, the JVM's
  default trust.
- Certificates are named by id or by subject DN. One that is not in the store keeps the connection
  `unavailable`, saying which, and the connection is tried again as the store fills up (at startup,
  the store may not be loaded yet).
- `trust_all` trusts any broker: for tests only. Kafka has no such thing: it is refused there.
- `hostname_verification` checks that the certificate of the broker holds the name it was reached
  with.

How each client gets the certificates:

| Broker | TLS on | Certificates |
|---|---|---|
| Kafka | `security_protocol` `SSL` or `SASL_SSL` | inline PEM properties (no file), or key stores in files |
| RabbitMQ | `tls.enabled` (port 5671 by default) | key and trust managers, in memory |
| Pulsar | a `pulsar+ssl://` url | PEM files of its own, deleted when it closes |
| Redis | a `rediss://` url, or `tls.enabled` (`start_tls` too) | key and trust managers, in memory |
| MQTT | `tls.enabled` | key and trust managers, in memory |
| Artemis | `tls.enabled` | PKCS12 files of its own, deleted when it closes |

Certificates and keys never go to a shared place on disk: the clients that only read files get files
of their own, in a temporary directory, removed with them.

## Authentication

| Broker | How |
|---|---|
| Kafka | SASL: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` with a `username` and a `password`, or a whole `jaas_config` (`OAUTHBEARER`, `GSSAPI`) |
| RabbitMQ | `username`, `password` (and the `virtual_host`) |
| Pulsar | `auth`: a token (a JWT), or `basic` |
| Redis | an ACL user: `username` and `password`, or the `password` of the `default` user |
| MQTT | `username`, `password` |
| Artemis | `username`, `password` |

Secrets are better kept in a [vault](https://www.otoroshi.io/docs/topics/secrets):
`"password": "${vault://local/brokers/kafka_password}"`. A broker that refuses the credentials keeps the
connection `unavailable`, with its answer in the [status](./overview.md#status): `WRONGPASS`,
`ACCESS_REFUSED`, `Authentication failed`, `AMQ229031`.

Mutual TLS and authentication are tested against each broker, with a CA and certificates created in
the store the way an operator would.
