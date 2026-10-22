package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.plugins.{CallOptions, EventBrokerPluginConfig}
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

  test("the options of the route survive a write and a read") {
    val config = EventBrokerPluginConfig("c1", Map("topic" -> "t"), allowedOverrides = Seq("take", "topic"), allowedTopics = Seq("orders-*"))
    assertEquals(EventBrokerPluginConfig.format.reads(config.json).get, config)
  }

  test("what the previous versions wrote is still read") {
    assertEquals(EventBrokerPluginConfig.format.reads(Json.obj("ref" -> "c1", "config" -> Json.obj("topic" -> "t"))).get.options, Map("topic" -> "t"))
    assertEquals(EventBrokerPluginConfig.format.reads(Json.obj("ref" -> "c1", "local_config" -> Json.obj("topic" -> "t"))).get.options, Map("topic" -> "t"))
  }

  test("by default the caller may only shape its own response") {
    val config = EventBrokerPluginConfig.default
    assertEquals(CallOptions.check(config, Map("take" -> "1", "format" -> "raw", "drop" -> "2", "duration" -> "3", "heartbeat" -> "4")), None)
    assertEquals(CallOptions.check(config, Map("topic" -> "t")), Some("the caller cannot set 'topic'"))
    assertEquals(CallOptions.check(config, Map("group" -> "g", "from" -> "earliest")), Some("the caller cannot set 'from', 'group'"))
  }

  test("what the route allows, the caller may set; * allows everything") {
    assertEquals(CallOptions.check(EventBrokerPluginConfig("c1", allowedOverrides = Seq("topic")), Map("topic" -> "t")), None)
    assertEquals(CallOptions.check(EventBrokerPluginConfig("c1", allowedOverrides = Seq("*")), Map("group" -> "g", "offset_from" -> "3")), None)
  }

  test("the topics a caller names must match the patterns of the route") {
    val config = EventBrokerPluginConfig("c1", allowedOverrides = Seq("*"), allowedTopics = Seq("orders-*", "invoices"))
    assertEquals(CallOptions.check(config, Map("topic" -> "orders-eu")), None)
    assertEquals(CallOptions.check(config, Map("queue" -> "invoices")), None)
    assertEquals(CallOptions.check(config, Map("topic" -> "secrets")), Some("the caller cannot use 'secrets'"))
    assertEquals(CallOptions.check(config, Map("topics" -> "orders-eu, payroll")), Some("the caller cannot use 'payroll'"))
    // a pattern is a glob, nothing else: regex characters are plain characters
    assert(!CallOptions.topicAllowed(EventBrokerPluginConfig("c1", allowedTopics = Seq("a.b")), "axb"))
  }
}
