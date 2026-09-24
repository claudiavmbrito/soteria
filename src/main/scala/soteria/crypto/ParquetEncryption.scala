package soteria.crypto

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.keytools.KeyToolkit

/** Settings for Parquet modular encryption (AES-GCM) with [[SoteriaKmsClient]]. */
object ParquetEncryption {

  val CryptoFactoryKey = "parquet.crypto.factory.class"
  val KmsClientKey = "parquet.encryption.kms.client.class"
  val UniformKey = "parquet.encryption.uniform.key"
  val AlgorithmKey = "parquet.encryption.algorithm"
  val PlaintextFooterKey = "parquet.encryption.plaintext.footer"

  /** Enables decryption on read, and encryption on writes that pass [[writeOptions]]. */
  def configure(conf: Configuration): Unit = {
    conf.set(CryptoFactoryKey, "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
    conf.set(KmsClientKey, classOf[SoteriaKmsClient].getName)
  }

  /** Writer options: encrypt all columns and the footer under `keyId`, with GCM for integrity. */
  def writeOptions(keyId: String): Map[String, String] = Map(
    UniformKey -> keyId,
    AlgorithmKey -> "AES_GCM_V1",
    PlaintextFooterKey -> "false"
  )

  /** Drops unwrapped keys cached by Parquet, e.g. after a master key is revoked. */
  def clearKeyCache(): Unit = KeyToolkit.removeCacheEntriesForAllTokens()
}
