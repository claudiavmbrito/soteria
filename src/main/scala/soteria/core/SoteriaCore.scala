package soteria.core

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import soteria.crypto.{ParquetEncryption, SoteriaKeyStore}
import soteria.partition.{ComputationPartitioner, SML2, SoteriaMode}
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import java.security.SecureRandom
import scala.reflect.ClassTag
import scala.util.Try

/**
 * SOTERIA Core Implementation
 * Based on the IEEE paper "Privacy-Preserving Machine Learning on Apache Spark"
 *
 * Datasets are stored as encrypted Parquet (AES-GCM, see [[soteria.crypto]]).
 * Computation is placed with stage-level scheduling (see
 * [[soteria.partition.ComputationPartitioner]]): by default every stage runs
 * on enclave executors; in SML-2, [[SoteriaSession.statistic]] lets
 * untrusted executors combine per-partition statistics.
 */
object SoteriaCore extends Logging {
  
  // Configuration constants
  private val ENCRYPTION_ALGORITHM = "AES"
  private val ENCRYPTION_TRANSFORMATION = "AES/GCM/NoPadding"
  private val GCM_IV_LENGTH = 12
  private val GCM_TAG_LENGTH = 16
  
  case class SoteriaConfig(
    // SML1 or SML2; if unset, taken from spark.soteria.mode (default SML2).
    mode: Option[SoteriaMode] = None,
    encryptionEnabled: Boolean = true,
    keySize: Int = 128,
    // Master key used for dataset encryption, resolved through SoteriaKeyStore.
    keyId: String = "soteria-master",
    // If false and no key is provisioned, an ephemeral key is generated (local development only).
    requireProvisionedKey: Boolean = false
  )
  
  /**
   * Dataset read from encrypted storage, with the master key that protects it.
   */
  case class EncryptedDataset[T](
    data: Dataset[T],
    encryptionKey: SecretKey
  )
  
  /**
   * AES-GCM encryption utilities
   */
  object EncryptionUtils {
    
    private lazy val random = new SecureRandom()
    
    def generateKey(keySize: Int = 128): SecretKey = {
      val keyGenerator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM)
      keyGenerator.init(keySize, random)
      keyGenerator.generateKey()
    }
    
    /** Returns `iv ++ ciphertext ++ tag`. `aad` is authenticated but not encrypted. */
    def encrypt(data: Array[Byte], key: SecretKey, aad: Array[Byte] = Array.emptyByteArray): Try[Array[Byte]] = Try {
      val cipher = Cipher.getInstance(ENCRYPTION_TRANSFORMATION)
      val iv = new Array[Byte](GCM_IV_LENGTH)
      random.nextBytes(iv)
      
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv))
      if (aad.nonEmpty) cipher.updateAAD(aad)
      
      iv ++ cipher.doFinal(data)
    }
    
    /** Fails if the ciphertext, tag or `aad` were tampered with. */
    def decrypt(encryptedData: Array[Byte], key: SecretKey, aad: Array[Byte] = Array.emptyByteArray): Try[Array[Byte]] = Try {
      require(encryptedData.length >= GCM_IV_LENGTH + GCM_TAG_LENGTH, "ciphertext too short")
      val iv = encryptedData.slice(0, GCM_IV_LENGTH)
      val cipherText = encryptedData.slice(GCM_IV_LENGTH, encryptedData.length)
      
      val cipher = Cipher.getInstance(ENCRYPTION_TRANSFORMATION)
      cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv))
      if (aad.nonEmpty) cipher.updateAAD(aad)
      
      cipher.doFinal(cipherText)
    }
  }
  
  /**
   * SOTERIA Session - main entry point for privacy-preserving ML
   */
  class SoteriaSession(val spark: SparkSession, val config: SoteriaConfig = SoteriaConfig()) {
    
    val mode: SoteriaMode = config.mode.getOrElse(
      spark.conf.getOption("spark.soteria.mode").map(SoteriaMode.parse).getOrElse(SML2))
    
    val partitioner = new ComputationPartitioner(spark.sparkContext, mode)
    
    val masterKey: SecretKey = resolveMasterKey()
    
    if (config.encryptionEnabled) ParquetEncryption.configure(spark.sparkContext.hadoopConfiguration)
    
    private def resolveMasterKey(): SecretKey = SoteriaKeyStore.get(config.keyId).getOrElse {
      require(!config.requireProvisionedKey, {
        val emptyEnv = sys.env.get(SoteriaKeyStore.EnvKeys).exists(_.trim.isEmpty)
        s"master key '${config.keyId}' was not provisioned (set ${SoteriaKeyStore.EnvKeys} or ${SoteriaKeyStore.EnvKeysFile})" +
          (if (emptyEnv) s"; ${SoteriaKeyStore.EnvKeys} is set but empty" else "")
      })
      logWarning(s"No master key '${config.keyId}' provisioned; using an ephemeral key. " +
        "Data encrypted in this session cannot be read by later sessions.")
      val key = EncryptionUtils.generateKey(config.keySize)
      SoteriaKeyStore.register(config.keyId, key)
      key
    }
    
    /**
     * Read an encrypted Parquet dataset. Every page and the footer are
     * authenticated with AES-GCM, so a tampered file fails to load.
     */
    def loadEncryptedDataset[T](path: String)(implicit encoder: org.apache.spark.sql.Encoder[T]): EncryptedDataset[T] = {
      EncryptedDataset(spark.read.parquet(path).as[T], masterKey)
    }
    
    /** Write `data` as Parquet with all columns and the footer encrypted under the session master key. */
    def saveEncrypted(data: Dataset[_], path: String, mode: SaveMode = SaveMode.ErrorIfExists): Unit = {
      val writer = data.write.mode(mode)
      val options = if (config.encryptionEnabled) ParquetEncryption.writeOptions(config.keyId) else Map.empty[String, String]
      writer.options(options).parquet(path)
    }
    
    /**
     * Folds each partition of `data` into a statistic inside the enclave and
     * combines the partial statistics (on untrusted executors in SML-2).
     * `seqOp` may mutate and return its accumulator.
     */
    def statistic[T, S: ClassTag](data: RDD[T])(zero: => S)(seqOp: (S, T) => S, combOp: (S, S) => S): S =
      partitioner.statistic(data)(zero)(seqOp, combOp)
    
    def close(): Unit = {
      spark.close()
    }
  }
  
  /**
   * Factory method to create SOTERIA session
   */
  def createSession(appName: String, config: SoteriaConfig = SoteriaConfig()): SoteriaSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Change local file permissions in-process: no chmod/chown child processes,
      // which cannot be started inside Gramine enclaves.
      .config("spark.hadoop.fs.file.impl", classOf[soteria.io.NioLocalFileSystem].getName)
      // Always fetch shuffle blocks from the executor that wrote them. With the
      // host-local shortcut, an executor opens another executor's shuffle files
      // directly; enclave executors keep theirs in a private in-enclave /tmp.
      .config("spark.shuffle.readHostLocalDisk", "false")
      .getOrCreate()
      
    new SoteriaSession(spark, config)
  }
}
