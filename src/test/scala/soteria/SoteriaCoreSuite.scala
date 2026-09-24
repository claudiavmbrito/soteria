package soteria

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.ml.linalg.Vectors
import soteria.core.SoteriaCore._
import soteria.ml.SoteriaML.ClassificationData

class SoteriaCoreSuite extends SparkTestBase {

  private val plaintext = "patient-42,diagnosis=positive".getBytes(UTF_8)

  test("AES-GCM round-trips and uses a fresh IV per message") {
    val key = EncryptionUtils.generateKey()
    val c1 = EncryptionUtils.encrypt(plaintext, key).get
    val c2 = EncryptionUtils.encrypt(plaintext, key).get
    assert(!java.util.Arrays.equals(c1, c2))
    assert(EncryptionUtils.decrypt(c1, key).get.sameElements(plaintext))
  }

  test("AES-GCM rejects tampered ciphertext, wrong key and wrong AAD") {
    val key = EncryptionUtils.generateKey()
    val aad = "dataset=train".getBytes(UTF_8)
    val ct = EncryptionUtils.encrypt(plaintext, key, aad).get

    val tampered = ct.clone()
    tampered(tampered.length / 2) = (tampered(tampered.length / 2) ^ 1).toByte
    assert(EncryptionUtils.decrypt(tampered, key, aad).isFailure)
    assert(EncryptionUtils.decrypt(ct, EncryptionUtils.generateKey(), aad).isFailure)
    assert(EncryptionUtils.decrypt(ct, key, "dataset=test".getBytes(UTF_8)).isFailure)
    assert(EncryptionUtils.decrypt(ct.take(10), key).isFailure)
    assert(EncryptionUtils.decrypt(ct, key, aad).get.sameElements(plaintext))
  }

  test("operations are classified into computation zones") {
    import ComputationPartitioner._
    assert(getComputationZone("gradient_computation") == EnclaveZone)
    assert(getComputationZone("MODEL_UPDATE") == EnclaveZone)
    assert(getComputationZone("result_aggregation") == UntrustedZone)
  }

  test("loadEncryptedDataset reads Parquet and executeWithPartitioning runs the computation") {
    val spark = session.spark
    import spark.implicits._
    val dir = Files.createTempDirectory("soteria").resolve("data").toString
    Seq(ClassificationData(Vectors.dense(1.0, 2.0), 1.0), ClassificationData(Vectors.dense(0.0, 0.0), 0.0))
      .toDS().write.parquet(dir)

    val ds = session.loadEncryptedDataset[ClassificationData](dir)
    assert(ds.encryptionKey == session.masterKey)
    val labelSum = session.executeWithPartitioning(ds, "model_update", (d: org.apache.spark.sql.Dataset[ClassificationData]) =>
      d.collect().map(_.label).sum)
    assert(labelSum == 1.0)
  }
}
