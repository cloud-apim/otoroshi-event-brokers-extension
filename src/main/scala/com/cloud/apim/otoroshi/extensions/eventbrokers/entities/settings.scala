package com.cloud.apim.otoroshi.extensions.eventbrokers.entities

import io.lettuce.core.RedisURI
import otoroshi.events.{KafkaConfig, PulsarConfig, SaslConfig}
import otoroshi.utils.http.MtlsConfig
import play.api.libs.json.*

import scala.collection.mutable
import scala.util.Try

/** what is wrong with the settings of a connection, one message per field */
final class InvalidConnectionSettings(val errors: Seq[String])
    extends IllegalArgumentException(s"invalid settings: ${errors.mkString(", ")}")

/**
 * Reads one json object field by field, and collects what is wrong (with the path of the field)
 * instead of stopping at the first error. Fields it was not asked for are errors too: a typo, or a
 * field of an older format, must not be silently ignored.
 */
private[entities] final class Fields(json: JsObject, path: String, errors: mutable.ListBuffer[String]) {

  private val seen = mutable.Set.empty[String]

  private def at(key: String): String = if (path.isEmpty) key else s"$path.$key"

  def error(key: String, message: String): Unit = errors += s"${at(key)}: $message"

  /** fields accepted and not read: what another variant of the object uses */
  def allow(keys: String*): Unit = seen ++= keys

  /** a value that will come from a vault: its format can only be checked once resolved */
  private def isSecretRef(value: JsValue): Boolean = value match {
    case JsString(s) => s.contains("${vault://")
    case _           => false
  }

  def opt[A](key: String)(using reads: Reads[A]): Option[A] = {
    seen += key
    json.value.get(key) match {
      case None | Some(JsNull) => None
      case Some(value)         =>
        value.validate[A] match {
          case JsSuccess(a, _) => Some(a)
          case JsError(errs)   =>
            error(key, errs.flatMap(_._2).flatMap(_.messages).distinct.mkString(", ").replace("error.expected.", "expected "))
            None
        }
    }
  }

  def get[A](key: String, default: => A)(using Reads[A]): A = opt[A](key).getOrElse(default)

  def required[A](key: String)(using Reads[A]): Option[A] = {
    val present = json.value.get(key).exists(_ != JsNull)
    val value   = opt[A](key)
    if (!present) error(key, "required")
    value
  }

  def requiredString(key: String): Option[String] = required[String](key).filter { s =>
    if (s.trim.isEmpty) error(key, "must not be empty")
    s.trim.nonEmpty
  }

  def optString(key: String): Option[String] = opt[String](key).filter(_.trim.nonEmpty)

  def oneOf(key: String, default: String, values: Seq[String]): String = {
    val value = get[String](key, default)
    if (!values.contains(value)) error(key, s"must be one of ${values.mkString(", ")}")
    value
  }

  def intIn(key: String, default: Int, min: Int, max: Int): Int = {
    val value = get[Int](key, default)
    if (value < min || value > max) error(key, s"must be between $min and $max")
    value
  }

  /** checks a value, unless it comes from a vault */
  def check(key: String, valid: Boolean, message: => String): Unit =
    if (!json.value.get(key).exists(isSecretRef) && !valid) error(key, message)

  def obj[A](key: String)(f: Fields => A): A = {
    seen += key
    val nested = json.value.get(key) match {
      case Some(o: JsObject)   => o
      case None | Some(JsNull) => Json.obj()
      case Some(_)             =>
        error(key, "expected an object")
        Json.obj()
    }
    val fields = new Fields(nested, at(key), errors)
    val result = f(fields)
    fields.done()
    result
  }

  def done(): Unit = json.keys.filterNot(seen.contains).toSeq.sorted.foreach(key => error(key, "unknown field"))
}

private[entities] object Fields {
  def read[A](json: JsObject)(f: Fields => A): Either[Seq[String], A] = {
    val errors = mutable.ListBuffer.empty[String]
    val fields = new Fields(json, "", errors)
    val result = f(fields)
    fields.done()
    if (errors.isEmpty) Right(result) else Left(errors.toList)
  }
}

/**
 * TLS with certificates from the Otoroshi certificate store. `enabled` turns it on; without
 * certificates the JVM's default trust store is used.
 */
final case class TlsSettings(
    enabled: Boolean = false,
    clientCerts: Seq[String] = Seq.empty,
    trustedCerts: Seq[String] = Seq.empty,
    trustAll: Boolean = false,
    hostnameVerification: Boolean = true
) {
  def mtlsConfig: MtlsConfig = MtlsConfig(
    certs = clientCerts,
    trustedCerts = trustedCerts,
    mtls = enabled,
    loose = !hostnameVerification,
    trustAll = trustAll
  )
  def json: JsObject = Json.obj(
    "enabled"               -> enabled,
    "client_certs"          -> clientCerts,
    "trusted_certs"         -> trustedCerts,
    "trust_all"             -> trustAll,
    "hostname_verification" -> hostnameVerification
  )
}

object TlsSettings {
  /** the common fields, for the tls objects that add their own */
  def readFields(f: Fields): TlsSettings = TlsSettings(
    enabled = f.get[Boolean]("enabled", false),
    clientCerts = f.get[Seq[String]]("client_certs", Seq.empty),
    trustedCerts = f.get[Seq[String]]("trusted_certs", Seq.empty),
    trustAll = f.get[Boolean]("trust_all", false),
    hostnameVerification = f.get[Boolean]("hostname_verification", true)
  )
}

/** the settings of a connection, one kind of them per kind of broker */
sealed trait BrokerSettings {
  def kind: EventBrokerConnectionKind
  def json: JsObject
}

object BrokerSettings {

  def read(kind: EventBrokerConnectionKind, json: JsObject): Either[Seq[String], BrokerSettings] = kind match {
    case EventBrokerConnectionKind.Kafka    => KafkaSettings.read(json)
    case EventBrokerConnectionKind.RabbitMQ => RabbitMQSettings.read(json)
    case EventBrokerConnectionKind.Pulsar   => PulsarSettings.read(json)
    case EventBrokerConnectionKind.Redis    => RedisSettings.read(json)
    case EventBrokerConnectionKind.MQTT     => MqttSettings.read(json)
  }

  /** a starting point for a new connection: what the backoffice form shows first */
  def template(kind: EventBrokerConnectionKind): BrokerSettings = kind match {
    case EventBrokerConnectionKind.Kafka    => KafkaSettings(bootstrapServers = Seq("localhost:9092"), defaultTopic = Some("events"))
    case EventBrokerConnectionKind.RabbitMQ => RabbitMQSettings(addresses = Seq("localhost:5672"), defaultQueue = Some("events"))
    case EventBrokerConnectionKind.Pulsar   => PulsarSettings(serviceUrl = "pulsar://localhost:6650", defaultTopic = Some("events"))
    case EventBrokerConnectionKind.Redis    => RedisSettings(uri = "redis://localhost:6379", defaultChannel = Some("events"))
    case EventBrokerConnectionKind.MQTT     => MqttSettings(host = "localhost", defaultTopic = Some("events"))
  }
}

// -------------------------------------------------------------------------------------------------
// kafka
// -------------------------------------------------------------------------------------------------

final case class KafkaSasl(
    mechanism: String = "PLAIN",
    username: Option[String] = None,
    password: Option[String] = None,
    jaasConfig: Option[String] = None
) {
  def json: JsObject = Json.obj("mechanism" -> mechanism, "username" -> username, "password" -> password, "jaas_config" -> jaasConfig)
}

/** tls with certificates from the store, or with keystore and truststore files */
final case class KafkaTls(
    tls: TlsSettings = TlsSettings(),
    keystorePath: Option[String] = None,
    truststorePath: Option[String] = None,
    storePassword: Option[String] = None
) {
  def json: JsObject = tls.json ++ Json.obj(
    "keystore_path"   -> keystorePath,
    "truststore_path" -> truststorePath,
    "store_password"  -> storePassword
  )
}

final case class KafkaSettings(
    bootstrapServers: Seq[String],
    defaultTopic: Option[String] = None,
    securityProtocol: String = "PLAINTEXT",
    sasl: KafkaSasl = KafkaSasl(),
    tls: KafkaTls = KafkaTls(),
    /** raw kafka client properties, applied last */
    properties: Map[String, String] = Map.empty
) extends BrokerSettings {

  def kind: EventBrokerConnectionKind = EventBrokerConnectionKind.Kafka

  def usesSasl: Boolean = securityProtocol.startsWith("SASL_")

  /** what otoroshi's own kafka support reads: its properties builder handles every tls and sasl case */
  def kafkaConfig: KafkaConfig = KafkaConfig(
    servers = bootstrapServers,
    keyPass = tls.storePassword,
    keystore = tls.keystorePath,
    truststore = tls.truststorePath,
    sendEvents = false,
    hostValidation = tls.tls.hostnameVerification,
    topic = defaultTopic.getOrElse(""),
    mtlsConfig = tls.tls.mtlsConfig,
    securityProtocol = securityProtocol,
    saslConfig =
      if (usesSasl) Some(SaslConfig(sasl.username.getOrElse(""), sasl.password.getOrElse(""), sasl.mechanism, sasl.jaasConfig))
      else None,
    properties = Some(properties).filter(_.nonEmpty)
  )

  def json: JsObject = Json.obj(
    "bootstrap_servers" -> bootstrapServers,
    "default_topic"     -> defaultTopic,
    "security_protocol" -> securityProtocol,
    "sasl"              -> sasl.json,
    "tls"               -> tls.json,
    "properties"        -> properties
  )
}

object KafkaSettings {

  val securityProtocols: Seq[String] = Seq("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
  val saslMechanisms: Seq[String]    = Seq("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI")

  def read(json: JsObject): Either[Seq[String], KafkaSettings] = Fields.read(json) { f =>
    val servers  = f.required[Seq[String]]("bootstrap_servers").getOrElse(Seq.empty)
    if (f.opt[JsValue]("bootstrap_servers").isDefined && servers.isEmpty) f.error("bootstrap_servers", "must not be empty")
    servers.filterNot(s => s.contains("${vault://") || s.matches("^[^\\s:/]+:\\d+$")).foreach { s =>
      f.error("bootstrap_servers", s"'$s' is not a host:port")
    }
    val protocol = f.oneOf("security_protocol", "PLAINTEXT", securityProtocols)
    val sasl     = f.obj("sasl") { s =>
      KafkaSasl(
        mechanism = s.oneOf("mechanism", "PLAIN", saslMechanisms),
        username = s.optString("username"),
        password = s.optString("password"),
        jaasConfig = s.optString("jaas_config")
      )
    }
    if (protocol.startsWith("SASL_") && sasl.jaasConfig.isEmpty && (sasl.username.isEmpty || sasl.password.isEmpty)) {
      f.error("sasl", s"$protocol needs a username and a password, or a jaas_config")
    }
    val tls      = f.obj("tls") { t =>
      KafkaTls(
        tls = TlsSettings.readFields(t),
        keystorePath = t.optString("keystore_path"),
        truststorePath = t.optString("truststore_path"),
        storePassword = t.optString("store_password")
      )
    }
    if ((tls.tls.enabled || tls.keystorePath.isDefined || tls.truststorePath.isDefined) && !protocol.endsWith("SSL")) {
      f.error("tls", s"tls needs security_protocol SSL or SASL_SSL, not $protocol")
    }
    if (tls.keystorePath.isDefined != tls.truststorePath.isDefined || (tls.keystorePath.isDefined && tls.storePassword.isEmpty)) {
      f.error("tls", "keystore_path, truststore_path and store_password go together")
    }
    KafkaSettings(
      bootstrapServers = servers,
      defaultTopic = f.optString("default_topic"),
      securityProtocol = protocol,
      sasl = sasl,
      tls = tls,
      properties = f.get[Map[String, String]]("properties", Map.empty)
    )
  }
}

// -------------------------------------------------------------------------------------------------
// rabbitmq
// -------------------------------------------------------------------------------------------------

final case class RabbitMQSettings(
    addresses: Seq[String],
    virtualHost: String = "/",
    username: Option[String] = None,
    password: Option[String] = None,
    defaultQueue: Option[String] = None
) extends BrokerSettings {

  def kind: EventBrokerConnectionKind = EventBrokerConnectionKind.RabbitMQ

  def json: JsObject = Json.obj(
    "addresses"     -> addresses,
    "virtual_host"  -> virtualHost,
    "username"      -> username,
    "password"      -> password,
    "default_queue" -> defaultQueue
  )
}

object RabbitMQSettings {
  def read(json: JsObject): Either[Seq[String], RabbitMQSettings] = Fields.read(json) { f =>
    val addresses = f.required[Seq[String]]("addresses").getOrElse(Seq.empty)
    if (f.opt[JsValue]("addresses").isDefined && addresses.isEmpty) f.error("addresses", "must not be empty")
    addresses.filterNot(a => a.contains("${vault://") || a.matches("^[^\\s:/]+(:\\d+)?$")).foreach { a =>
      f.error("addresses", s"'$a' is not a host or a host:port")
    }
    RabbitMQSettings(
      addresses = addresses,
      virtualHost = f.get[String]("virtual_host", "/"),
      username = f.optString("username"),
      password = f.optString("password"),
      defaultQueue = f.optString("default_queue")
    )
  }
}

// -------------------------------------------------------------------------------------------------
// pulsar
// -------------------------------------------------------------------------------------------------

enum PulsarAuth {
  case Disabled
  case Token(token: String)
  case Basic(username: String, password: String)

  def json: JsObject = this match {
    case Disabled                  => Json.obj("kind" -> "none")
    case Token(token)              => Json.obj("kind" -> "token", "token" -> token)
    case Basic(username, password) => Json.obj("kind" -> "basic", "username" -> username, "password" -> password)
  }
}

final case class PulsarSettings(
    serviceUrl: String,
    tenant: String = "public",
    namespace: String = "default",
    defaultTopic: Option[String] = None,
    auth: PulsarAuth = PulsarAuth.Disabled,
    tls: TlsSettings = TlsSettings(),
    /** a pem file of trusted certificates, on top of `tls.trusted_certs` */
    tlsTrustCertsFilePath: Option[String] = None
) extends BrokerSettings {

  def kind: EventBrokerConnectionKind = EventBrokerConnectionKind.Pulsar

  def fullTopic(topic: String): String =
    if (topic.contains("://")) topic else s"persistent://$tenant/$namespace/$topic"

  /** what otoroshi's own pulsar support reads */
  def pulsarConfig: PulsarConfig = PulsarConfig(
    uri = serviceUrl,
    tlsTrustCertsFilePath = tlsTrustCertsFilePath,
    tenant = tenant,
    namespace = namespace,
    topic = defaultTopic.getOrElse(""),
    token = None,
    username = None,
    password = None,
    mtlsConfig = tls.mtlsConfig
  )

  def json: JsObject = Json.obj(
    "service_url"              -> serviceUrl,
    "tenant"                   -> tenant,
    "namespace"                -> namespace,
    "default_topic"            -> defaultTopic,
    "auth"                     -> auth.json,
    "tls"                      -> (tls.json ++ Json.obj("trust_certs_file_path" -> tlsTrustCertsFilePath))
  )
}

object PulsarSettings {
  def read(json: JsObject): Either[Seq[String], PulsarSettings] = Fields.read(json) { f =>
    val url              = f.requiredString("service_url").getOrElse("")
    f.check("service_url", url.isEmpty || url.startsWith("pulsar://") || url.startsWith("pulsar+ssl://"), "must start with pulsar:// or pulsar+ssl://")
    val auth             = f.obj("auth") { a =>
      // the form keeps the fields of the other kinds around
      a.allow("token", "username", "password")
      a.oneOf("kind", "none", Seq("none", "token", "basic")) match {
        case "token" => PulsarAuth.Token(a.requiredString("token").getOrElse(""))
        case "basic" => PulsarAuth.Basic(a.requiredString("username").getOrElse(""), a.requiredString("password").getOrElse(""))
        case _       => PulsarAuth.Disabled
      }
    }
    val (tls, trustCertsFile) = f.obj("tls")(t => (TlsSettings.readFields(t), t.optString("trust_certs_file_path")))
    if (tls.enabled && url.startsWith("pulsar://")) f.error("tls", "tls needs a pulsar+ssl:// service_url")
    PulsarSettings(
      serviceUrl = url,
      tenant = f.get[String]("tenant", "public"),
      namespace = f.get[String]("namespace", "default"),
      defaultTopic = f.optString("default_topic"),
      auth = auth,
      tls = tls,
      tlsTrustCertsFilePath = trustCertsFile
    )
  }
}

// -------------------------------------------------------------------------------------------------
// redis
// -------------------------------------------------------------------------------------------------

final case class RedisSettings(
    /** redis://, or rediss:// for tls */
    uri: String,
    username: Option[String] = None,
    password: Option[String] = None,
    defaultChannel: Option[String] = None,
    startTls: Boolean = false,
    verifyPeer: Boolean = true,
    ioThreadPoolSize: Option[Int] = None,
    computationThreadPoolSize: Option[Int] = None
) extends BrokerSettings {

  def kind: EventBrokerConnectionKind = EventBrokerConnectionKind.Redis

  def json: JsObject = Json.obj(
    "uri"                          -> uri,
    "username"                     -> username,
    "password"                     -> password,
    "default_channel"              -> defaultChannel,
    "tls"                          -> Json.obj("start_tls" -> startTls, "verify_peer" -> verifyPeer),
    "io_thread_pool_size"          -> ioThreadPoolSize,
    "computation_thread_pool_size" -> computationThreadPoolSize
  )
}

object RedisSettings {
  def read(json: JsObject): Either[Seq[String], RedisSettings] = Fields.read(json) { f =>
    val uri                = f.requiredString("uri").getOrElse("")
    f.check("uri", uri.isEmpty || Try(RedisURI.create(uri)).isSuccess, "is not a redis uri")
    val (startTls, verifyPeer) = f.obj("tls")(t => (t.get[Boolean]("start_tls", false), t.get[Boolean]("verify_peer", true)))
    RedisSettings(
      uri = uri,
      username = f.optString("username"),
      password = f.optString("password"),
      defaultChannel = f.optString("default_channel"),
      startTls = startTls,
      verifyPeer = verifyPeer,
      ioThreadPoolSize = f.opt[Int]("io_thread_pool_size"),
      computationThreadPoolSize = f.opt[Int]("computation_thread_pool_size")
    )
  }
}

// -------------------------------------------------------------------------------------------------
// mqtt
// -------------------------------------------------------------------------------------------------

final case class MqttSettings(
    host: String,
    port: Int = 1883,
    /** 5, or 3 for mqtt 3.1.1 */
    version: Int = 5,
    /** tcp, or websocket */
    transport: String = "tcp",
    websocketPath: String = "/mqtt",
    tls: TlsSettings = TlsSettings(),
    /** each client gets this prefix and a random suffix: every node needs a client id of its own */
    clientIdPrefix: String = "otoroshi",
    username: Option[String] = None,
    password: Option[String] = None,
    defaultTopic: Option[String] = None,
    defaultQos: Int = 0,
    retain: Boolean = false,
    cleanSession: Boolean = true,
    /** mqtt 5: how long the broker keeps the session once disconnected */
    sessionExpirySeconds: Long = 0L,
    connectionTimeoutSeconds: Int = 10,
    keepAliveSeconds: Int = 60
) extends BrokerSettings {

  def kind: EventBrokerConnectionKind = EventBrokerConnectionKind.MQTT

  def isV5: Boolean = version == 5

  def json: JsObject = Json.obj(
    "host"                       -> host,
    "port"                       -> port,
    "version"                    -> version,
    "transport"                  -> transport,
    "websocket_path"             -> websocketPath,
    "tls"                        -> tls.json,
    "client_id_prefix"           -> clientIdPrefix,
    "username"                   -> username,
    "password"                   -> password,
    "default_topic"              -> defaultTopic,
    "default_qos"                -> defaultQos,
    "retain"                     -> retain,
    "clean_session"              -> cleanSession,
    "session_expiry_seconds"     -> sessionExpirySeconds,
    "connection_timeout_seconds" -> connectionTimeoutSeconds,
    "keep_alive_seconds"         -> keepAliveSeconds
  )
}

object MqttSettings {
  def read(json: JsObject): Either[Seq[String], MqttSettings] = Fields.read(json) { f =>
    val prefix  = f.get[String]("client_id_prefix", "otoroshi")
    // mqtt 3.1.1 servers only have to accept 23 characters: the random suffix takes 9
    f.check("client_id_prefix", prefix.length <= 14 && prefix.matches("^[0-9a-zA-Z-]*$"), "at most 14 letters, digits or dashes")
    val version = f.get[Int]("version", 5)
    if (version != 3 && version != 5) f.error("version", "must be 3 (mqtt 3.1.1) or 5")
    val expiry  = f.get[Long]("session_expiry_seconds", 0L)
    if (expiry < 0 || expiry > 4294967295L) f.error("session_expiry_seconds", "must be between 0 and 4294967295")
    if (expiry > 0 && version != 5) f.error("session_expiry_seconds", "is an mqtt 5 setting")
    val path    = f.get[String]("websocket_path", "/mqtt")
    f.check("websocket_path", path.startsWith("/"), "must start with /")
    MqttSettings(
      host = f.requiredString("host").getOrElse(""),
      port = f.intIn("port", 1883, 1, 65535),
      version = version,
      transport = f.oneOf("transport", "tcp", Seq("tcp", "websocket")),
      websocketPath = path,
      tls = f.obj("tls")(TlsSettings.readFields),
      clientIdPrefix = prefix,
      username = f.optString("username"),
      password = f.optString("password"),
      defaultTopic = f.optString("default_topic"),
      defaultQos = f.intIn("default_qos", 0, 0, 2),
      retain = f.get[Boolean]("retain", false),
      cleanSession = f.get[Boolean]("clean_session", true),
      sessionExpirySeconds = expiry,
      connectionTimeoutSeconds = f.intIn("connection_timeout_seconds", 10, 1, 300),
      keepAliveSeconds = f.intIn("keep_alive_seconds", 60, 0, 65535)
    )
  }
}
