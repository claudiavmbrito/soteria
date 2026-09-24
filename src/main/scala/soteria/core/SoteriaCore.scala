package soteria.core

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import soteria.crypto.{ParquetEncryption, SoteriaKeyStore}
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import java.security.SecureRandom
import scala.util.Try

/**
 * SOTERIA Core Implementation
 * Based on the IEEE paper "Privacy-Preserving Machine Learning on Apache Spark"
 *
 * Status (v2.0 rebuild, phase 1): datasets are stored as encrypted Parquet
 * (AES-GCM, see [[soteria.crypto]]). The computation zones below are only
 * classified and logged: nothing is isolated in an SGX enclave yet. Enclave
 * scheduling (Gramine + stage-level scheduling) is added in later phases.
 */
object SoteriaCore extends Logging {
  
  // Configuration constants
  private val ENCRYPTION_ALGORITHM = "AES"
  private val ENCRYPTION_TRANSFORMATION = "AES/GCM/NoPadding"
  private val GCM_IV_LENGTH = 12
  private val GCM_TAG_LENGTH = 16
  
  case class SoteriaConfig(
    enclaveEnabled: Boolean = true,
    encryptionEnabled: Boolean = true,
    partitioningStrategy: String = "COMPUTATION_PARTITIONING", // or "BASELINE"
    keySize: Int = 128,
    batchSize: Int = 1000,
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
   * Computation partitioning manager
   * Decides which operations run inside SGX enclaves vs outside
   */
  object ComputationPartitioner {
    
    sealed trait ComputationZone
    case object EnclaveZone extends ComputationZone
    case object UntrustedZone extends ComputationZone
    
    /**
     * Determines computation zone based on operation sensitivity
     */
    def getComputationZone(operation: String): ComputationZone = operation match {
      case op if isSensitiveOperation(op) => EnclaveZone
      case _ => UntrustedZone
    }
    
    private def isSensitiveOperation(operation: String): Boolean = {
      val sensitiveOps = Set(
        "gradient_computation",
        "model_update", 
        "feature_extraction",
        "data_preprocessing",
        "model_inference"
      )
      sensitiveOps.contains(operation.toLowerCase)
    }
  }
  
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
    
    val masterKey: SecretKey = resolveMasterKey()
    
    if (config.encryptionEnabled) ParquetEncryption.configure(spark.sparkContext.hadoopConfiguration)
    
    private def resolveMasterKey(): SecretKey = SoteriaKeyStore.get(config.keyId).getOrElse {
      require(!config.requireProvisionedKey,
        s"master key '${config.keyId}' was not provisioned (set ${SoteriaKeyStore.EnvKeys} or ${SoteriaKeyStore.EnvKeysFile})")
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
     * Run `computation` in the zone chosen for `operation`.
     * Zones are not enforced yet: both run on the regular Spark executors.
     */
    def executeWithPartitioning[T, R](
      dataset: EncryptedDataset[T], 
      operation: String,
      computation: Dataset[T] => R
    ): R = {
      val zone = ComputationPartitioner.getComputationZone(operation)
      logInfo(s"$operation -> $zone (zone not enforced: no enclave backend configured)")
      computation(dataset.data)
    }
    
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
      .getOrCreate()
      
    new SoteriaSession(spark, config)
  }
}
