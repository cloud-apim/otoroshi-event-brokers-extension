package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.EventBrokerPluginConfig
import play.api.libs.json.*

class PluginConfigSuite extends munit.FunSuite {

  test("defaults when the config is empty") {
    val config = EventBrokerPluginConfig.format.reads(Json.obj()).get
    assertEquals(config, EventBrokerPluginConfig.default)
  }

  test("reads the connection ref and the include flags") {
    val config = EventBrokerPluginConfig.format.reads(Json.obj("ref" -> "c1", "include_headers" -> false, "include_query" -> false)).get
    assertEquals(config.ref, "c1")
    assertEquals(config.includeHeaders, false)
    assertEquals(config.includeQuery, false)
  }

  // written as `config` (which is also what the form edits) but read as `local_config`
  test("the local options survive a write and a read".fail) {
    val config = EventBrokerPluginConfig("c1", Map("topic" -> "t"), includeHeaders = true, includeQuery = true)
    assertEquals(EventBrokerPluginConfig.format.reads(config.json).get, config)
  }
}
