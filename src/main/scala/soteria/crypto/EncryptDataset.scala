package soteria.crypto

import soteria.core.SoteriaCore
import soteria.core.SoteriaCore.SoteriaConfig

/**
 * Client-side setup (Figure 1 of the SOTERIA proof): encrypts a dataset into
 * encrypted Parquet before it is uploaded to untrusted storage.
 *
 * {{{
 * SOTERIA_MASTER_KEYS=soteria-master:<base64> spark-submit --class soteria.crypto.EncryptDataset \
 *   soteria.jar <input> <output> [--format parquet|csv|json] [--key-id soteria-master]
 * }}}
 *
 * Run it where the plaintext already lives (the data owner's machine), never
 * on the untrusted cluster.
 */
object EncryptDataset {

  case class Args(input: String, output: String, format: String = "parquet", keyId: String = SoteriaConfig().keyId)

  def parseArgs(args: Array[String]): Args = {
    def loop(rest: List[String], acc: Args): Args = rest match {
      case "--format" :: f :: tail => loop(tail, acc.copy(format = f))
      case "--key-id" :: k :: tail => loop(tail, acc.copy(keyId = k))
      case Nil => acc
      case other => throw new IllegalArgumentException(s"unexpected arguments: ${other.mkString(" ")}")
    }
    args.toList match {
      case in :: out :: rest if !in.startsWith("--") && !out.startsWith("--") => loop(rest, Args(in, out))
      case _ => throw new IllegalArgumentException(
        "usage: EncryptDataset <input> <output> [--format parquet|csv|json] [--key-id id]")
    }
  }

  def main(args: Array[String]): Unit = {
    val a = parseArgs(args)
    val session = SoteriaCore.createSession(
      "soteria-encrypt", SoteriaConfig(keyId = a.keyId, requireProvisionedKey = true))
    try {
      val reader = session.spark.read
      val df = a.format match {
        case "csv" => reader.option("header", "true").option("inferSchema", "true").csv(a.input)
        case f => reader.format(f).load(a.input)
      }
      session.saveEncrypted(df, a.output)
    } finally session.close()
  }
}
