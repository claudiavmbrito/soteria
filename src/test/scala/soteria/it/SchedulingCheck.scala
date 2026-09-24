package soteria.it

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import soteria.core.SoteriaCore
import soteria.core.SoteriaCore.SoteriaConfig
import soteria.ml.ExtendedSoteriaML.SoteriaGBT
import soteria.ml.SoteriaML._
import soteria.partition.{ComputationPartitioner, SML2}

/**
 * End-to-end check of SML-2 placement on a real standalone cluster with one
 * enclave worker (advertises the enclave resource and holds the master key)
 * and one untrusted worker. Run by scripts/local-cluster/run.sh.
 *
 * Asserts, from Spark's own scheduler events, that every task of a
 * default-profile stage ran on an enclave executor, and that untrusted
 * executors only ran statistic-combine stages.
 */
object SchedulingCheck {

  def main(args: Array[String]): Unit = {
    val dataDir = args.headOption.getOrElse(sys.error("usage: SchedulingCheck <scratch dir>"))
    val session = SoteriaCore.createSession("soteria-scheduling-check",
      SoteriaConfig(mode = Some(SML2), requireProvisionedKey = true))
    val sc = session.spark.sparkContext

    val enclaveExecutors = ConcurrentHashMap.newKeySet[String]()
    val allExecutors = ConcurrentHashMap.newKeySet[String]()
    val stageProfile = new ConcurrentHashMap[Int, Int]()
    val tasks = new java.util.concurrent.ConcurrentLinkedQueue[(Int, String)]()
    sc.addSparkListener(new SparkListener {
      override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit = {
        allExecutors.add(e.executorId)
        if (e.executorInfo.resourcesInfo.contains(ComputationPartitioner.EnclaveResource)) enclaveExecutors.add(e.executorId)
      }
      override def onStageSubmitted(s: SparkListenerStageSubmitted): Unit =
        stageProfile.put(s.stageInfo.stageId, s.stageInfo.resourceProfileId)
      override def onTaskEnd(t: SparkListenerTaskEnd): Unit = tasks.add((t.stageId, t.taskInfo.executorId))
    })

    val spark = session.spark
    import spark.implicits._
    val rnd = new scala.util.Random(1)
    val cls = (0 until 2000).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      ClassificationData(Vectors.dense(x1, x2), if (x1 - x2 > 0) 1.0 else 0.0)
    }
    val reg = (0 until 500).map { _ =>
      val x = rnd.nextGaussian(); RegressionData(Vectors.dense(x), 3 * x + 1)
    }

    // Encrypted storage: only processes holding the master key can read these.
    session.saveEncrypted(cls.toDS().repartition(8), s"$dataDir/cls.enc")
    session.saveEncrypted(reg.toDS().repartition(4), s"$dataDir/reg.enc")

    val lr = new SoteriaLogisticRegression(session, maxIterations = 20)
      .train(session.loadEncryptedDataset[ClassificationData](s"$dataDir/cls.enc"))
    val accuracy = cls.count(p => lr.predict(p.features) == p.label).toDouble / cls.size
    val lin = new SoteriaLinearRegression(session).train(session.loadEncryptedDataset[RegressionData](s"$dataDir/reg.enc"))
    new SoteriaGBT(session, maxIter = 3).train(session.loadEncryptedDataset[RegressionData](s"$dataDir/reg.enc"))
    Thread.sleep(2000) // let the listener bus drain

    val defaultId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    val placed = tasks.asScala.toSeq.map { case (stage, exec) => (stageProfile.get(stage), exec) }
    val defaultTasks = placed.filter(_._1 == defaultId)
    val untrustedTasks = placed.filter(_._1 != defaultId)
    val leaked = defaultTasks.filterNot(t => enclaveExecutors.contains(t._2))
    val onUntrustedExecutors = placed.filterNot(t => enclaveExecutors.contains(t._2))

    println(s"executors: ${allExecutors.size} total, ${enclaveExecutors.size} with the enclave resource (ids ${enclaveExecutors.asScala.toSeq.sorted.mkString(", ")})")
    println(s"tasks: ${placed.size} total, ${defaultTasks.size} enclave-profile, ${untrustedTasks.size} untrusted-profile")
    println(s"tasks that ran on untrusted executors: ${onUntrustedExecutors.size}")
    println(f"LR training accuracy: $accuracy%.3f; linear model: ${lin.coefficients(0)}%.4f x + ${lin.intercept}%.4f")

    val failures = Seq(
      "an enclave-profile task ran on an untrusted executor" -> leaked.nonEmpty,
      "no statistic combine ran with the untrusted profile" -> untrustedTasks.isEmpty,
      "no task ran on an untrusted executor (the check proves nothing)" -> onUntrustedExecutors.isEmpty,
      "an untrusted executor ran something other than a statistic combine" -> onUntrustedExecutors.exists(_._1 == defaultId),
      "logistic regression did not train" -> (accuracy < 0.95),
      "linear regression did not train" -> (math.abs(lin.coefficients(0) - 3) > 1e-6)
    ).collect { case (msg, true) => msg }

    session.close()
    if (failures.nonEmpty) {
      failures.foreach(f => println(s"FAIL: $f"))
      sys.exit(1)
    }
    println("PASS: SML-2 placement verified on a standalone cluster")
  }
}
