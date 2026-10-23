---
title: Configuration
description: Every configuration key of the extension, its default and its environment variable.
---

# Configuration

Under `otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers`:

| Key | Default | Environment variable | |
|---|---|---|---|
| `enabled` | `true` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_ENABLED` | the extension itself |
| `connections.connect-timeout` | `10s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_CONNECT_TIMEOUT` | how long one connection attempt may take |
| `connections.retry.min-backoff` | `1s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_RETRY_MIN_BACKOFF` | the first wait before trying again |
| `connections.retry.max-backoff` | `30s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_RETRY_MAX_BACKOFF` | the longest wait (twice as long each time, 20% jitter) |
| `connections.healthcheck.interval` | `10s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_HEALTHCHECK_INTERVAL` | how often a connected client is checked |
| `connections.healthcheck.timeout` | `5s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_HEALTHCHECK_TIMEOUT` | how long a check may take |
| `connections.healthcheck.max-failures` | `3` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_HEALTHCHECK_MAX_FAILURES` | failed checks in a row before the client is built again |
| `connections.retire-grace-period` | `30s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_RETIRE_GRACE_PERIOD` | how long the streams open on a replaced client may go on |
| `connections.close-timeout` | `10s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_CLOSE_TIMEOUT` | how long closing a client may take |
| `webhooks.enabled` | `true` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_WEBHOOKS_ENABLED` | whether this node delivers webhooks |
| `webhooks.redis.uri` | none | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_WEBHOOKS_REDIS_URI` | the redis the nodes of a cluster share: required in a cluster |
| `webhooks.lock-ttl` | `15s` | `CLOUD_APIM_EXTENSIONS_EVENT_BROKERS_WEBHOOKS_LOCK_TTL` | how long a node keeps a webhook without renewing it |
| `webhooks.node-kinds` | `["leader", "worker"]` | | the kinds of nodes that deliver webhooks, in a cluster |

```hocon
otoroshi.admin-extensions.configurations.cloud-apim_extensions_eventbrokers {
  enabled = true
  connections {
    connect-timeout = 10s
    retry.min-backoff = 1s
    retry.max-backoff = 30s
    healthcheck.interval = 10s
    healthcheck.timeout = 5s
    healthcheck.max-failures = 3
    retire-grace-period = 30s
    close-timeout = 10s
  }
  webhooks {
    enabled = true
    redis.uri = "redis://shared-redis:6379"
    lock-ttl = 15s
    node-kinds = ["leader", "worker"]
  }
}
```
