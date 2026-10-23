---
sidebar_position: 3
title: Install
description: The jar, the requirements, and the configuration of the extension.
---

# Install

## Requirements

- Otoroshi **18.0.0** or later
- Java **17** or later

## The jar

Each [release](https://github.com/cloud-apim/otoroshi-event-brokers-extension/releases/latest) has
`otoroshi-event-brokers-extension_3-<version>.jar` and its `.sha256`:

```bash
sha256sum -c otoroshi-event-brokers-extension_3-<version>.jar.sha256
```

Put it on Otoroshi's classpath:

```bash
java -cp "./otoroshi-event-brokers-extension_3-<version>.jar:./otoroshi.jar" play.core.server.ProdServerStart
```

or mount it into the plugins directory of the Otoroshi Docker image (`OTOROSHI_PLUGINS_DIR_PATH`).

The jar carries the RabbitMQ and MQTT clients; the Kafka, Pulsar, Redis and Artemis ones come from
Otoroshi.

## Enabling

The extension is enabled as soon as its jar is loaded. To turn it off:

```hocon
otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers.enabled = false
```

or `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_ENABLED=false`.

## Configuration

Everything else has defaults: how the connections are kept, and how the webhooks are coordinated in a
cluster. See [Configuration](./reference/configuration.md) for every key and its environment
variable.

In a **cluster**, the nodes that deliver webhooks need a Redis they share:

```hocon
otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers.webhooks.redis.uri = "redis://shared-redis:6379"
```

See [Running in a cluster](./operations/cluster.md).
