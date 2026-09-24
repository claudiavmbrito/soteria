package soteria.crypto

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.KeyAccessDeniedException
import org.apache.parquet.crypto.keytools.KmsClient
import soteria.core.SoteriaCore.EncryptionUtils

/**
 * Parquet KMS client that wraps data keys with SOTERIA master keys.
 *
 * Parquet modular encryption encrypts every page and the footer with AES-GCM
 * data keys and stores those keys, wrapped, in the file. This client does the
 * wrapping with AES-GCM under a master key from [[SoteriaKeyStore]], binding
 * the master key id as associated data. A process without the master key
 * (e.g. an untrusted executor) cannot unwrap the data keys and so cannot read
 * the file.
 */
class SoteriaKmsClient extends KmsClient {

  override def initialize(conf: Configuration, kmsInstanceID: String, kmsInstanceURL: String, accessToken: String): Unit = ()

  override def wrapKey(keyBytes: Array[Byte], masterKeyIdentifier: String): String = {
    val wrapped = EncryptionUtils
      .encrypt(keyBytes, masterKey(masterKeyIdentifier), masterKeyIdentifier.getBytes(UTF_8))
      .getOrElse(throw new KeyAccessDeniedException(s"could not wrap key with master key '$masterKeyIdentifier'"))
    Base64.getEncoder.encodeToString(wrapped)
  }

  override def unwrapKey(wrappedKey: String, masterKeyIdentifier: String): Array[Byte] = {
    EncryptionUtils
      .decrypt(Base64.getDecoder.decode(wrappedKey), masterKey(masterKeyIdentifier), masterKeyIdentifier.getBytes(UTF_8))
      .getOrElse(throw new KeyAccessDeniedException(s"could not unwrap key with master key '$masterKeyIdentifier'"))
  }

  private def masterKey(id: String) =
    SoteriaKeyStore.get(id).getOrElse(
      throw new KeyAccessDeniedException(s"master key '$id' is not available in this process"))
}
