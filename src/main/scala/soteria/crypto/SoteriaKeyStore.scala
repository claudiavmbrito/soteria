package soteria.crypto

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

import scala.collection.JavaConverters._

/**
 * Master keys available to this JVM, looked up by identifier.
 *
 * Keys are never read from the Spark or Hadoop configuration, because Spark
 * ships those to every executor. They come from, in order:
 *  1. keys registered in-process with [[register]] (local mode and tests);
 *  2. the `SOTERIA_MASTER_KEYS` environment variable;
 *  3. the file named by `SOTERIA_MASTER_KEYS_FILE`.
 *
 * Sources 2 and 3 use the format `id:base64key`, separated by commas or
 * newlines. In an SGX deployment they are filled in by Gramine secret
 * provisioning after remote attestation, so only attested enclaves hold keys.
 */
object SoteriaKeyStore {

  val EnvKeys = "SOTERIA_MASTER_KEYS"
  val EnvKeysFile = "SOTERIA_MASTER_KEYS_FILE"

  private val registered = new ConcurrentHashMap[String, SecretKey]()

  def register(id: String, key: SecretKey): Unit = {
    require(id.nonEmpty && !id.exists(c => c == ':' || c == ',' || c.isWhitespace), s"invalid key id '$id'")
    registered.put(id, key)
  }

  def unregister(id: String): Unit = registered.remove(id)

  def get(id: String): Option[SecretKey] =
    Option(registered.get(id)).orElse(provisioned(sys.env.get _).get(id))

  /** Keys from the environment variable or key file, as provisioned to this process. */
  private[soteria] def provisioned(env: String => Option[String]): Map[String, SecretKey] = {
    val fromEnv = env(EnvKeys).map(parse).getOrElse(Map.empty)
    val fromFile = env(EnvKeysFile)
      .map(p => parse(new String(Files.readAllBytes(Paths.get(p)), UTF_8)))
      .getOrElse(Map.empty)
    fromFile ++ fromEnv
  }

  private[soteria] def parse(spec: String): Map[String, SecretKey] =
    spec.split("[,\\n]").map(_.trim).filter(_.nonEmpty).map { entry =>
      entry.split(":", 2) match {
        case Array(id, b64) if id.nonEmpty =>
          val bytes = Base64.getDecoder.decode(b64.trim)
          require(Set(16, 24, 32).contains(bytes.length), s"key '$id' must be 128, 192 or 256 bits")
          id -> (new SecretKeySpec(bytes, "AES"): SecretKey)
        case _ => throw new IllegalArgumentException("master key entries must look like id:base64key")
      }
    }.toMap

  /** Formats a key for `SOTERIA_MASTER_KEYS`. */
  def format(id: String, key: SecretKey): String =
    s"$id:${Base64.getEncoder.encodeToString(key.getEncoded)}"

  private[soteria] def registeredIds: Set[String] = registered.keySet().asScala.toSet
}
