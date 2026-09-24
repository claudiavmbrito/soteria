package soteria.partition

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import org.apache.spark.{ShuffleDependency, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, ResourceProfileBuilder, TaskResourceRequests}

/**
 * Places stages into computation zones with Spark stage-level scheduling.
 *
 * The application's default resource profile is the enclave one: its
 * executors must hold the custom resource [[ComputationPartitioner.EnclaveResource]],
 * which only workers running inside Gramine-SGX advertise. Every stage that
 * is not explicitly placed elsewhere therefore runs in an enclave. This is
 * fail-safe: library code (e.g. MLlib trainers) cannot leak by forgetting to
 * mark a stage.
 *
 * In SML-2, [[statistic]] is the only way out: it computes per-partition
 * aggregates in the enclave stage and combines them in a stage that uses the
 * untrusted profile, after [[LeakageAuditor]] has checked the lineage.
 *
 * With a `local` master, profiles are not supported by Spark; placements are
 * then audited and counted but not applied.
 */
class ComputationPartitioner(sc: SparkContext, val mode: SoteriaMode) extends Logging {
  import ComputationPartitioner._

  val auditor = new LeakageAuditor

  private val applyProfiles = !sc.master.startsWith("local[") && sc.master != "local"
  private val untrustedCount = new AtomicInteger()

  if (applyProfiles) validateClusterConf()
  else logWarning(s"master ${sc.master}: zones are audited but not enforced (no stage-level scheduling in local mode)")

  /** Executors of this profile carry no enclave resource and receive no keys. */
  lazy val untrustedProfile: ResourceProfile = {
    val conf = sc.getConf
    val executor = new ExecutorResourceRequests()
      .cores(conf.getInt(UntrustedCoresKey, conf.getInt("spark.executor.cores", 1)))
      .memory(conf.get(UntrustedMemoryKey, conf.get("spark.executor.memory", "1g")))
    new ResourceProfileBuilder().require(executor).require(new TaskResourceRequests().cpus(1)).build()
  }

  /** Number of stages placed in the untrusted zone so far (for tests and reports). */
  def untrustedPlacements: Int = untrustedCount.get()

  /** Places `rdd`'s stage in the untrusted zone (SML-2) after checking it only sees statistics. */
  def untrusted[T](rdd: RDD[T]): RDD[T] = mode match {
    case SML1 => rdd
    case SML2 =>
      auditor.checkUntrusted(rdd)
      untrustedCount.incrementAndGet()
      if (applyProfiles) rdd.withResources(untrustedProfile) else rdd
  }

  /**
   * Folds each partition of `data` into a statistic inside the enclave, then
   * combines the per-partition statistics (in the untrusted zone under SML-2)
   * and returns the result to the driver. In SML-2 the per-partition
   * statistics are exactly what untrusted executors learn.
   */
  def statistic[T, S: ClassTag](data: RDD[T])(zero: => S)(seqOp: (S, T) => S, combOp: (S, S) => S): S = {
    // Each task deserializes its own copy of `z`, so seqOp may mutate it.
    val z = zero
    val partials = data.mapPartitions(it => Iterator(it.foldLeft(z)(seqOp)))
    val fanIn = math.max(1, math.ceil(math.sqrt(partials.getNumPartitions.toDouble)).toInt)
    val keyed = partials.mapPartitionsWithIndex((i, it) => it.map(s => (i % fanIn, s)))
    val combined = keyed.reduceByKey(combOp, fanIn)
    combined.dependencies.head match {
      case dep: ShuffleDependency[_, _, _] => auditor.registerStatisticShuffle(dep)
      case other => throw new IllegalStateException(s"expected a shuffle, got $other")
    }
    untrusted(combined.values).fold(z)(combOp) // fold clones z on the driver
  }

  private def validateClusterConf(): Unit = {
    val conf = sc.getConf
    val missing = Seq(
      s"spark.executor.resource.$EnclaveResource.amount",
      s"spark.task.resource.$EnclaveResource.amount"
    ).filterNot(conf.contains)
    require(missing.isEmpty,
      s"SOTERIA needs the enclave resource on the default profile; missing: ${missing.mkString(", ")}")
    if (mode == SML2) require(conf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = false),
      "SML-2 needs spark.dynamicAllocation.enabled=true for stage-level scheduling")
  }
}

object ComputationPartitioner {
  /** Custom Spark resource advertised only by workers running inside SGX enclaves. */
  val EnclaveResource = "enclave"
  val UntrustedCoresKey = "spark.soteria.untrusted.executor.cores"
  val UntrustedMemoryKey = "spark.soteria.untrusted.executor.memory"

}
