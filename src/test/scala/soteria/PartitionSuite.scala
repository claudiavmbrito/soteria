package soteria

import org.apache.spark.ml.linalg.Vectors
import soteria.core.SoteriaCore.EncryptedDataset
import soteria.ml.ExtendedSoteriaML.SoteriaGBT
import soteria.ml.SoteriaML._
import soteria.partition._

class PartitionSuite extends SparkTestBase {

  private def sc = session.spark.sparkContext

  test("modes parse from configuration strings") {
    assert(SoteriaMode.parse("sml1") == SML1)
    assert(SoteriaMode.parse("SML-2") == SML2)
    intercept[IllegalArgumentException](SoteriaMode.parse("SML3"))
  }

  test("sessions default to SML-2") {
    assert(session.mode == SML2)
  }

  test("statistic combines per-partition folds, with a fresh mutable zero per partition") {
    val data = sc.parallelize(1 to 1000, 7)
    val stats = session.statistic(data)(new Array[Double](2))(
      (acc, x) => { acc(0) += x; acc(1) += 1; acc },
      (a, b) => { a(0) += b(0); a(1) += b(1); a })
    assert(stats.toSeq == Seq(500500.0, 1000.0))
  }

  test("SML-2 places only statistic combines on untrusted executors; SML-1 places none") {
    val data = sc.parallelize(1 to 100, 4)
    val sml2 = new ComputationPartitioner(sc, SML2)
    val sml1 = new ComputationPartitioner(sc, SML1)
    assert(sml2.statistic(data)(0)(_ + _, _ + _) == 5050)
    assert(sml1.statistic(data)(0)(_ + _, _ + _) == 5050)
    assert(sml2.untrustedPlacements == 1)
    assert(sml1.untrustedPlacements == 0)
  }

  test("auditor rejects raw records reaching an untrusted stage") {
    val p = new ComputationPartitioner(sc, SML2)
    val raw = sc.parallelize(Seq(("alice", 1.0), ("bob", 2.0)), 2)

    // narrow: same stage as the raw scan
    intercept[LeakageException](p.untrusted(raw.map(_._2)))
    // wide, but the shuffle carries raw records
    intercept[LeakageException](p.untrusted(raw.groupByKey()))
    // raw data joined in after a legitimate statistic
    val stat = sc.parallelize(Seq(1, 2), 2).map(x => (x % 2, x)).reduceByKey(_ + _)
    intercept[LeakageException](p.untrusted(stat.union(raw.map(r => (0, r._2.toInt)))))
    assert(p.untrustedPlacements == 0)
  }

  test("auditor accepts public inputs") {
    val p = new ComputationPartitioner(sc, SML2)
    val reference = sc.parallelize(1 to 10, 2)
    p.auditor.declarePublic(reference)
    assert(p.untrusted(reference.map(_ * 2)).sum() == 110)
  }

  test("partitioned trainers go through statistics; enclave-only trainers never leave the enclave") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new scala.util.Random(1)
    val cls = (0 until 200).map { _ =>
      val x = rnd.nextGaussian(); ClassificationData(Vectors.dense(x), if (x > 0) 1.0 else 0.0)
    }
    val reg = (0 until 200).map { _ =>
      val x = rnd.nextGaussian(); RegressionData(Vectors.dense(x), 2 * x)
    }

    val before = session.partitioner.untrustedPlacements
    new SoteriaLogisticRegression(session, maxIterations = 5).train(EncryptedDataset(cls.toDS(), session.masterKey))
    val afterLr = session.partitioner.untrustedPlacements
    assert(afterLr > before)

    new SoteriaGBT(session, maxIter = 2).train(EncryptedDataset(reg.toDS(), session.masterKey))
    assert(session.partitioner.untrustedPlacements == afterLr)
  }
}
