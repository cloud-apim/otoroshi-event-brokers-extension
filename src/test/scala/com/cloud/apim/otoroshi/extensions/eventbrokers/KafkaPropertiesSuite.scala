package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.clients.KafkaBrokerClient
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.*

/** the client properties of the security settings of a kafka connection */
class KafkaPropertiesSuite extends munit.FunSuite {

  val base = KafkaSettings(bootstrapServers = Seq("k:9092"))

  test("plaintext: the protocol, and nothing else") {
    assertEquals(KafkaBrokerClient.properties(base), Map("security.protocol" -> "PLAINTEXT"))
  }

  test("sasl: the login module of the mechanism, with the credentials quoted") {
    val scram = KafkaBrokerClient.properties(base.copy(securityProtocol = "SASL_PLAINTEXT", sasl = KafkaSasl("SCRAM-SHA-512", Some("alice"), Some("p\"a\\ss"))))
    assertEquals(scram("sasl.mechanism"), "SCRAM-SHA-512")
    assertEquals(scram("sasl.jaas.config"), """org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="p\"a\\ss";""")
    val plain = KafkaBrokerClient.properties(base.copy(securityProtocol = "SASL_SSL", sasl = KafkaSasl("PLAIN", Some("bob"), Some("x"))))
    assert(plain("sasl.jaas.config").startsWith("org.apache.kafka.common.security.plain.PlainLoginModule required"), plain.toString)
    val jaas  = KafkaBrokerClient.properties(base.copy(securityProtocol = "SASL_SSL", sasl = KafkaSasl("OAUTHBEARER", jaasConfig = Some("custom;"))))
    assertEquals(jaas("sasl.jaas.config"), "custom;")
  }

  test("ssl with key stores in files, without hostname verification") {
    val props = KafkaBrokerClient.properties(
      base.copy(securityProtocol = "SSL", tls = KafkaTls(TlsSettings(hostnameVerification = false), Some("/k.jks"), Some("/t.jks"), Some("secret")))
    )
    assertEquals(props("ssl.keystore.location"), "/k.jks")
    assertEquals(props("ssl.truststore.password"), "secret")
    assertEquals(props("ssl.endpoint.identification.algorithm"), "")
    assert(!props.contains("sasl.mechanism"), props.toString)
  }

  test("the raw properties of the connection come last") {
    assertEquals(KafkaBrokerClient.properties(base.copy(properties = Map("security.protocol" -> "SSL")))("security.protocol"), "SSL")
  }
}
