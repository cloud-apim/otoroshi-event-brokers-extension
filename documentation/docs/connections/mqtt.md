---
title: MQTT
description: MQTT 5 and 3.1.1, over TCP or websockets, through the HiveMQ client.
---

# MQTT

```json
{
  "host": "mosquitto",
  "port": 1883,
  "version": 5,
  "transport": "tcp",
  "websocket_path": "/mqtt",
  "tls": { "enabled": false, "client_certs": [], "trusted_certs": [], "trust_all": false, "hostname_verification": true },
  "client_id_prefix": "otoroshi",
  "username": null,
  "password": null,
  "default_topic": null,
  "default_qos": 0,
  "retain": false,
  "clean_session": true,
  "session_expiry_seconds": 0,
  "connection_timeout_seconds": 10,
  "keep_alive_seconds": 60
}
```

`host` is required.

- `version` is `5` or `3` (MQTT 3.1.1: no properties, no shared subscriptions).
- `transport` is `tcp` or `websocket` (on `websocket_path`).
- Every client gets its own id, the prefix and a random suffix: two nodes of a cluster never share one
  (a broker drops the older of two clients with the same id). The prefix is at most 14 characters, for
  the 23 MQTT 3.1.1 servers have to accept.
- Published messages are not retained unless asked (`retain`, or the `retained` option): a retained
  message is replayed to every new subscriber.
- A clean session loses its subscriptions with its connection: the client reconnects by itself (1s to
  30s) and makes them again. `session_expiry_seconds` (MQTT 5) is how long the broker keeps a session
  once disconnected.

## What MQTT brings

- **MQTT 5**: the attributes of an event go in user properties, its content type is the MQTT content
  type; a `group` is a shared subscription (`$share/<group>/<topic>`), whose subscribers share the
  messages. `response_topic` and message expiry are set from what the event says.
- **MQTT 3.1.1**: no properties: the whole event goes as JSON in the payload.
- `qos` (`0`, `1`, `2`) per call, `default_qos` otherwise. Wildcards (`+`, `#`) in what a consumer reads.
- No acks and no replay: a subscription gets what is published while it is there.
