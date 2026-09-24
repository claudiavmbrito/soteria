package soteria.partition

/**
 * SOTERIA deployment modes from the paper.
 *
 *  - [[SML1]] (SOTERIA-B): every stage runs inside enclaves.
 *  - [[SML2]] (SOTERIA-P): stages that touch raw data run inside enclaves;
 *    stages that only combine statistics may run on untrusted executors.
 */
sealed trait SoteriaMode
case object SML1 extends SoteriaMode
case object SML2 extends SoteriaMode

object SoteriaMode {
  def parse(s: String): SoteriaMode = s.trim.toUpperCase match {
    case "SML1" | "SML-1" | "BASELINE" => SML1
    case "SML2" | "SML-2" | "PARTITIONED" => SML2
    case other => throw new IllegalArgumentException(s"unknown SOTERIA mode '$other' (use SML1 or SML2)")
  }
}

/** Where a stage is allowed to run. */
sealed trait Zone
object Zone {
  case object Enclave extends Zone
  case object Untrusted extends Zone
}
