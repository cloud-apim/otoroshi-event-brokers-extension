package com.cloud.apim.otoroshi.extensions.eventbrokers

import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.TlsSettings
import otoroshi.ssl.Cert
import otoroshi.utils.http.MtlsConfig

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.cert.X509Certificate
import java.security.{KeyStore, SecureRandom}
import java.util.{Base64, UUID}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.util.Try

/** `trust_all`: for the clients that take a factory, a factory of a manager that trusts anything */
object TrustAll {
  private val manager = new javax.net.ssl.X509TrustManager {
    override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
  }
  val factory: TrustManagerFactory = new TrustManagerFactory(
    new javax.net.ssl.TrustManagerFactorySpi {
      override def engineInit(ks: KeyStore): Unit = ()
      override def engineInit(spec: javax.net.ssl.ManagerFactoryParameters): Unit = ()
      override def engineGetTrustManagers(): Array[javax.net.ssl.TrustManager] = Array(manager)
    },
    new java.security.Provider("cloud-apim-trust-all", "1.0", "trusts every certificate") {},
    "trust-all"
  ) {}
}

/**
 * What the tls settings of a connection name, out of the otoroshi certificate store (by id, or by
 * subject dn): who the client is (the first of `client_certs`, with its private key) and whom it
 * trusts (`trusted_certs`; none: the jvm's default trust, `trust_all`: anyone). Resolved when a
 * client starts: a certificate not in the store fails the start, which the manager tries again (the
 * store may not be loaded yet when otoroshi starts).
 *
 * The clients that take key and trust managers get them in memory. Those that only take files get
 * files of their own (`files()`), that they delete when they close.
 */
final class TlsMaterial private (val settings: TlsSettings, val client: Option[(Seq[X509Certificate], java.security.PrivateKey)], val trusted: Seq[X509Certificate]) {

  private lazy val password: String = UUID.randomUUID().toString.replace("-", "")

  /** the client certificate and its key, as a pkcs12 store */
  lazy val keyStore: Option[KeyStore] = client.map { (chain, key) =>
    val store = KeyStore.getInstance("PKCS12")
    store.load(null, null)
    store.setKeyEntry("client", key, password.toCharArray, chain.toArray)
    store
  }

  /** the certificates trusted, as a pkcs12 store */
  lazy val trustStore: Option[KeyStore] = Option.when(trusted.nonEmpty) {
    val store = KeyStore.getInstance("PKCS12")
    store.load(null, null)
    trusted.zipWithIndex.foreach((cert, i) => store.setCertificateEntry(s"trusted-$i", cert))
    store
  }

  lazy val keyManagers: Option[KeyManagerFactory] = keyStore.map { store =>
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(store, password.toCharArray)
    kmf
  }

  /** none: the jvm's default trust */
  lazy val trustManagers: Option[TrustManagerFactory] =
    if (settings.trustAll) Some(TrustAll.factory)
    else
      trustStore.map { store =>
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(store)
        tmf
      }

  lazy val sslContext: SSLContext = {
    val context = SSLContext.getInstance("TLS")
    context.init(keyManagers.map(_.getKeyManagers).orNull, trustManagers.map(_.getTrustManagers).orNull, new SecureRandom())
    context
  }

  /** the chain of the client certificate, as pem */
  lazy val clientChainPem: Option[String] = client.map((chain, _) => chain.map(TlsMaterial.pem).mkString)

  /** the private key of the client certificate, as unencrypted pkcs8 pem */
  lazy val clientKeyPem: Option[String] = client.map((_, key) => TlsMaterial.pem("PRIVATE KEY", key.getEncoded))

  lazy val trustedPem: Option[String] = Option.when(trusted.nonEmpty)(trusted.map(TlsMaterial.pem).mkString)

  /** files for the clients that only read files, in a directory of their own: `delete()` once closed */
  def files(): TlsFiles = {
    val dir = Files.createTempDirectory("cloud-apim-event-brokers-tls-")
    def write(name: String, content: Array[Byte]): File = {
      val file = dir.resolve(name).toFile
      val out  = new FileOutputStream(file)
      try out.write(content)
      finally out.close()
      file
    }
    def store(name: String, ks: KeyStore): File = {
      val file = dir.resolve(name).toFile
      val out  = new FileOutputStream(file)
      try ks.store(out, password.toCharArray)
      finally out.close()
      file
    }
    new TlsFiles(
      dir = dir,
      password = password,
      keyStore = keyStore.map(store("client.p12", _)),
      trustStore = trustStore.map(store("trusted.p12", _)),
      clientChainPem = clientChainPem.map(p => write("client.pem", p.getBytes(StandardCharsets.UTF_8))),
      clientKeyPem = clientKeyPem.map(p => write("client-key.pem", p.getBytes(StandardCharsets.UTF_8))),
      trustedPem = trustedPem.map(p => write("trusted.pem", p.getBytes(StandardCharsets.UTF_8)))
    )
  }
}

/** the files of a client: pkcs12 stores (all with the same password) and their pem equivalents */
final class TlsFiles(
    dir: Path,
    val password: String,
    val keyStore: Option[File],
    val trustStore: Option[File],
    val clientChainPem: Option[File],
    val clientKeyPem: Option[File],
    val trustedPem: Option[File]
) {
  def delete(): Unit = Try {
    Option(dir.toFile.listFiles()).toSeq.flatten.foreach(_.delete())
    dir.toFile.delete()
  }
}

object TlsMaterial {

  private def pem(cert: X509Certificate): String = pem("CERTIFICATE", cert.getEncoded)

  private def pem(kind: String, der: Array[Byte]): String =
    s"-----BEGIN $kind-----\n${Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)}\n-----END $kind-----\n"

  private def lookup(what: String, refs: Seq[String], resolve: Seq[String] => Seq[Cert]): Seq[Cert] =
    refs.distinct.map { ref =>
      resolve(Seq(ref)).headOption.getOrElse(throw new IllegalStateException(s"$what '$ref' is not in the certificate store (yet?)"))
    }

  def resolve(settings: TlsSettings): TlsMaterial = {
    val clients = lookup("the client certificate", settings.clientCerts, refs => MtlsConfig(certs = refs).actualCerts)
    val trusted = lookup("the trusted certificate", settings.trustedCerts, refs => MtlsConfig(trustedCerts = refs).actualTrustedCerts)
    val client  = clients.headOption.map { cert =>
      if (cert.privateKey.trim.isEmpty) throw new IllegalStateException(s"the client certificate '${cert.id}' has no private key")
      (cert.certificatesChain.toSeq, cert.cryptoKeyPair.getPrivate)
    }
    new TlsMaterial(settings, client, trusted.flatMap(_.certificatesChain.toSeq))
  }
}
