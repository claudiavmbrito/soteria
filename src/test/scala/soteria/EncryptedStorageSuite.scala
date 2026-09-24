package soteria

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Base64

import org.apache.parquet.crypto.KeyAccessDeniedException
import org.apache.spark.ml.linalg.Vectors
import soteria.core.SoteriaCore.EncryptionUtils
import soteria.crypto.{EncryptDataset, ParquetEncryption, SoteriaKeyStore, SoteriaKmsClient}
import soteria.ml.SoteriaML.ClassificationData

class EncryptedStorageSuite extends SparkTestBase {

  private val Marker = "SOTERIA-PLAINTEXT-MARKER"

  private def tempDir(): Path = Files.createTempDirectory("soteria-enc")

  private def dataFiles(dir: String): Seq[File] =
    new File(dir).listFiles().filter(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet")).toSeq

  private def containsMarker(dir: String): Boolean =
    dataFiles(dir).exists(f => new String(Files.readAllBytes(f.toPath), UTF_8).contains(Marker))

  private def causes(t: Throwable): Seq[Throwable] = Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq

  private def markerData() = {
    val spark = session.spark
    import spark.implicits._
    (1 to 50).map(i => (i, s"$Marker-$i", Vectors.dense(i.toDouble, -i.toDouble))).toDF("id", "note", "features")
  }

  test("encrypted Parquet round-trips typed ML data") {
    val spark = session.spark
    import spark.implicits._
    val out = tempDir().resolve("train").toString
    val rows = Seq(ClassificationData(Vectors.dense(1.0, 2.0), 1.0), ClassificationData(Vectors.dense(3.0, 4.0), 0.0))
    session.saveEncrypted(rows.toDS(), out)

    val loaded = session.loadEncryptedDataset[ClassificationData](out)
    assert(loaded.data.collect().sortBy(_.label).toSeq == rows.sortBy(_.label))
  }

  test("encrypted files contain no plaintext, unlike a plain write") {
    val base = tempDir()
    val plain = base.resolve("plain").toString
    val enc = base.resolve("enc").toString
    markerData().write.parquet(plain)
    session.saveEncrypted(markerData(), enc)

    assert(containsMarker(plain), "control: plaintext Parquet should expose the marker")
    assert(!containsMarker(enc), "encrypted Parquet leaked plaintext")
    assert(session.spark.read.parquet(enc).count() == 50)
  }

  test("tampering never yields different data: reads fail or are unaffected (GCM integrity)") {
    val out = tempDir().resolve("tamper").toString
    session.saveEncrypted(markerData().coalesce(1), out)
    val file = dataFiles(out).head
    new File(file.getParent, s".${file.getName}.crc").delete() // make GCM, not the Hadoop checksum, catch it
    val original = Files.readAllBytes(file.toPath)
    val expected = session.spark.read.parquet(out).collect().map(_.toString).sorted.toSeq

    // Parquet authenticates each module (page, header, index, footer) when it is read;
    // bytes of modules a query never reads (e.g. page indexes) cannot change its result.
    val positions = (4 until original.length - 8 by math.max(1, original.length / 40)).toSeq
    val detected = positions.count { pos =>
      val bytes = original.clone()
      bytes(pos) = (bytes(pos) ^ 0x01).toByte
      Files.write(file.toPath, bytes)
      scala.util.Try(session.spark.read.parquet(out).collect().map(_.toString).sorted.toSeq) match {
        case scala.util.Success(rows) =>
          assert(rows == expected, s"flipping byte $pos silently changed the data")
          false
        case scala.util.Failure(_) => true
      }
    }
    Files.write(file.toPath, original)
    assert(detected > positions.size / 2, s"only $detected of ${positions.size} tampered positions were detected")
  }

  test("a process without the master key cannot read the data") {
    val out = tempDir().resolve("nokey").toString
    session.saveEncrypted(markerData(), out)
    val keyId = session.config.keyId

    SoteriaKeyStore.unregister(keyId)
    ParquetEncryption.clearKeyCache()
    try {
      val e = intercept[Exception](session.spark.read.parquet(out).collect())
      assert(causes(e).exists(_.isInstanceOf[KeyAccessDeniedException]), s"unexpected failure: $e")
    } finally {
      SoteriaKeyStore.register(keyId, session.masterKey)
    }
    assert(session.spark.read.parquet(out).count() == 50)
  }

  test("KMS client binds wrapped keys to the master key id") {
    val kms = new SoteriaKmsClient
    SoteriaKeyStore.register("kms-a", EncryptionUtils.generateKey())
    SoteriaKeyStore.register("kms-b", EncryptionUtils.generateKey())
    val dek = Array.tabulate[Byte](16)(_.toByte)
    val wrapped = kms.wrapKey(dek, "kms-a")
    assert(kms.unwrapKey(wrapped, "kms-a").sameElements(dek))
    intercept[KeyAccessDeniedException](kms.unwrapKey(wrapped, "kms-b"))
    intercept[KeyAccessDeniedException](kms.unwrapKey(wrapped, "missing"))
  }

  test("key store parses provisioned keys from env and file") {
    val k1 = EncryptionUtils.generateKey(); val k2 = EncryptionUtils.generateKey(256)
    val file = Files.createTempFile("keys", ".txt")
    Files.write(file, (SoteriaKeyStore.format("file-key", k2) + "\n").getBytes(UTF_8))
    val env = Map(SoteriaKeyStore.EnvKeys -> SoteriaKeyStore.format("env-key", k1), SoteriaKeyStore.EnvKeysFile -> file.toString)

    val keys = SoteriaKeyStore.provisioned(env.get)
    assert(keys("env-key").getEncoded.sameElements(k1.getEncoded))
    assert(keys("file-key").getEncoded.sameElements(k2.getEncoded))
    val badLength = "x:" + Base64.getEncoder.encodeToString(new Array[Byte](5))
    intercept[IllegalArgumentException](SoteriaKeyStore.parse(badLength))
    intercept[IllegalArgumentException](SoteriaKeyStore.parse("no-separator"))
  }

  test("EncryptDataset parses its command line") {
    val a = EncryptDataset.parseArgs(Array("in.csv", "out", "--format", "csv", "--key-id", "k1"))
    assert(a == EncryptDataset.Args("in.csv", "out", "csv", "k1"))
    intercept[IllegalArgumentException](EncryptDataset.parseArgs(Array("only-one")))
  }
}
