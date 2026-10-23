package com.cloud.apim.otoroshi.extensions.eventbrokers.it

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.*
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.security.{KeyPairGenerator, KeyStore, PrivateKey, SecureRandom}
import java.time.Instant
import java.util.{Base64, Date}

/**
 * The certificates of the tls tests: a ca, a server certificate for `localhost` (and only that name:
 * reaching the broker through 127.0.0.1 is a name it does not hold), and a client certificate, both
 * signed by the ca.
 */
object Pki {

  val Password = "changeit"

  private val random = new SecureRandom()

  final case class Issued(certificate: X509Certificate, key: PrivateKey, issuer: Option[Issued]) {
    def chain: Seq[X509Certificate] = certificate +: issuer.map(_.chain).getOrElse(Seq.empty)
    def certPem: String             = pem("CERTIFICATE", certificate.getEncoded)
    def chainPem: String            = chain.map(c => pem("CERTIFICATE", c.getEncoded)).mkString
    /** pkcs8, unencrypted */
    def keyPem: String              = pem("PRIVATE KEY", key.getEncoded)

    /** the certificate, its chain and its key, as a pkcs12 key store */
    def keyStore: Array[Byte] = {
      val store = KeyStore.getInstance("PKCS12")
      store.load(null, null)
      store.setKeyEntry("key", key, Password.toCharArray, chain.toArray)
      bytes(store)
    }

    /** the certificate alone, as a pkcs12 trust store */
    def trustStore: Array[Byte] = {
      val store = KeyStore.getInstance("PKCS12")
      store.load(null, null)
      store.setCertificateEntry("trusted", certificate)
      bytes(store)
    }
  }

  private def bytes(store: KeyStore): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    store.store(out, Password.toCharArray)
    out.toByteArray
  }

  private def pem(kind: String, der: Array[Byte]): String =
    s"-----BEGIN $kind-----\n${Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)}\n-----END $kind-----\n"

  private def issue(cn: String, issuer: Option[Issued], ca: Boolean, purposes: Seq[KeyPurposeId], names: Seq[String]): Issued = {
    val generator = KeyPairGenerator.getInstance("RSA")
    generator.initialize(2048)
    val keys      = generator.generateKeyPair()
    val now       = Instant.now()
    val subject   = new X500Name(s"CN=$cn, O=Cloud APIM event brokers tests")
    val signer    = issuer.map(i => X500Name.getInstance(i.certificate.getSubjectX500Principal.getEncoded)).getOrElse(subject)
    val builder   = new JcaX509v3CertificateBuilder(
      signer,
      BigInteger.valueOf(random.nextLong() & Long.MaxValue).add(BigInteger.ONE),
      Date.from(now.minusSeconds(3600)),
      Date.from(now.plusSeconds(30L * 24 * 3600)),
      subject,
      keys.getPublic
    )
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca))
    if (ca) builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign))
    else {
      builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
      builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(purposes.toArray))
    }
    if (names.nonEmpty) builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(names.map(n => new GeneralName(GeneralName.dNSName, n)).toArray))
    val signed    = builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(issuer.map(_.key).getOrElse(keys.getPrivate)))
    Issued(new JcaX509CertificateConverter().getCertificate(signed), keys.getPrivate, issuer)
  }

  lazy val ca: Issued     = issue("event brokers tests ca", None, ca = true, Seq.empty, Seq.empty)
  lazy val server: Issued = issue("localhost", Some(ca), ca = false, Seq(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth), Seq("localhost"))
  lazy val client: Issued = issue("otoroshi", Some(ca), ca = false, Seq(KeyPurposeId.id_kp_clientAuth), Seq.empty)
}
