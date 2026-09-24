package soteria.partition

import scala.collection.mutable

import org.apache.spark.{Dependency, NarrowDependency, ShuffleDependency}
import org.apache.spark.rdd.RDD

/** Raised when a computation would let raw data reach an untrusted stage. */
class LeakageException(msg: String) extends SecurityException(msg)

/**
 * Enforces the SML-2 leakage function: untrusted stages may only see
 * statistics, never raw records.
 *
 * An RDD placed in the untrusted zone is checked by walking its lineage.
 * Narrow dependencies stay in the same stage, so they are followed. A shuffle
 * carries the map side's output to the untrusted stage, so it is allowed only
 * if it was registered as a statistic shuffle (per-partition aggregates
 * produced inside the enclave), or if everything behind it is public.
 * Reaching any source RDD that was not declared public is a violation.
 */
class LeakageAuditor {

  private val statisticShuffles = mutable.Set.empty[Int]
  private val publicRdds = mutable.Set.empty[Int]

  def registerStatisticShuffle(dep: ShuffleDependency[_, _, _]): Unit = synchronized {
    statisticShuffles += dep.shuffleId
  }

  /** Declares an RDD as non-sensitive input (e.g. public reference data). */
  def declarePublic(rdd: RDD[_]): Unit = synchronized { publicRdds += rdd.id }

  /** Throws [[LeakageException]] if untrusted execution of `rdd` could expose raw data. */
  def checkUntrusted(rdd: RDD[_]): Unit = synchronized {
    val visited = mutable.Set.empty[Int]
    def visit(r: RDD[_], path: List[String]): Unit = {
      if (publicRdds.contains(r.id) || !visited.add(r.id)) return
      val here = s"${r.getClass.getSimpleName}[${r.id}]" :: path
      val deps: Seq[Dependency[_]] = r.dependencies
      if (deps.isEmpty) {
        throw new LeakageException(
          s"untrusted stage would read raw data from ${here.head} via ${here.reverse.mkString(" -> ")}; " +
            "reduce it to a statistic first (SoteriaSession.statistic)")
      }
      deps.foreach {
        case s: ShuffleDependency[_, _, _] if statisticShuffles.contains(s.shuffleId) => ()
        case s: ShuffleDependency[_, _, _] => visit(s.rdd, here)
        case n: NarrowDependency[_] => visit(n.rdd, here)
        case other => visit(other.rdd, here)
      }
    }
    visit(rdd, Nil)
  }
}
