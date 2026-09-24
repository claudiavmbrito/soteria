package soteria

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite
import soteria.core.SoteriaCore
import soteria.core.SoteriaCore.{EncryptionUtils, SoteriaConfig}
import soteria.crypto.{EncryptDataset, SoteriaKeyStore}

/** Runs whole applications; each one creates and stops its own Spark session. */
class SoteriaExamplesSuite extends AnyFunSuite {

  System.setProperty("spark.master", "local[2]")
  System.setProperty("spark.ui.enabled", "false")

  test("example application runs end to end") {
    examples.SoteriaExamples.main(Array.empty)
  }

  test("EncryptDataset encrypts a CSV that a session with the same key can read") {
    SoteriaKeyStore.register("cli-key", EncryptionUtils.generateKey())
    val dir = Files.createTempDirectory("soteria-cli")
    val csv = dir.resolve("in.csv")
    Files.write(csv, "id,value\n1,10.5\n2,20.5\n".getBytes(UTF_8))
    val out = dir.resolve("out").toString

    EncryptDataset.main(Array(csv.toString, out, "--format", "csv", "--key-id", "cli-key"))

    val session = SoteriaCore.createSession("reader", SoteriaConfig(keyId = "cli-key", requireProvisionedKey = true))
    try {
      val rows = session.spark.read.parquet(out).collect().map(r => (r.getInt(0), r.getDouble(1))).sorted
      assert(rows.toSeq == Seq((1, 10.5), (2, 20.5)))
    } finally session.close()
  }

  test("EncryptDataset refuses to run without a provisioned key") {
    intercept[IllegalArgumentException](
      EncryptDataset.main(Array("in", "out", "--key-id", "never-provisioned")))
  }
}
