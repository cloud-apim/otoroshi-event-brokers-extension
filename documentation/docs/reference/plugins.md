---
title: Plugins
description: The identifiers of the plugins of the extension.
---

# Plugins

| Plugin | Identifier |
|---|---|
| Cloud APIM - HTTP events ingester | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsIngester` |
| Cloud APIM - HTTP events consumer | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsConsumer` |
| Cloud APIM - SSE events consumer | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.SseEventConsumer` |
| Cloud APIM - Websocket Ingester/Consumer | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.IngesterConsumerWebsocket` |
| Cloud APIM - Events request reply | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsRequestReply` |
| Cloud APIM - Event brokers exporter (a data exporter) | `cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.EventBrokersDataExporter` |

A route with a plugin, through the admin API:

```json
{
  "plugins": [
    {
      "enabled": true,
      "plugin": "cp:otoroshi_plugins.com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.HttpEventsIngester",
      "config": { "ref": "connection_...", "options": { "topic": "orders" } }
    }
  ]
}
```

The workflow functions: `extensions.com.cloud-apim.event-brokers.publish` and
`extensions.com.cloud-apim.event-brokers.consume` ([Workflow functions](../features/workflows.md)).
