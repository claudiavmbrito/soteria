package soteria

import java.nio.file.Files

import soteria.bench.Bench
import soteria.bench.Bench.{Options, Runner}
import soteria.core.SoteriaCore.{SoteriaConfig, SoteriaSession}
import soteria.partition.SML1

/** Every benchmark workload runs on tiny data in vanilla, SML-1 and SML-2, with comparable quality. */
class BenchSuite extends SparkTestBase {

  private val expected = Map( // metric -> quality check
    "accuracy" -> ((q: Double) => q > 0.8),
    "rmse" -> ((q: Double) => q >= 0 && q < 2.0),
    "cost" -> ((q: Double) => q > 0),
    "explained_variance" -> ((q: Double) => q > 0.5 && q <= 1.0 + 1e-9),
    "log_perplexity" -> ((q: Double) => q > 0))

  test("argument parsing") {
    val o = Bench.parseArgs(Seq("--mode", "SML-2", "--data", "/d", "--algo", "lr,pca", "--scale", "0.5", "--reps", "2"))
    assert(o.mode == "sml2" && o.algorithms == Seq("lr", "pca") && o.scale == 0.5 && o.reps == 2 && o.warmup == 1)
    assert(Bench.parseArgs(Seq("--mode", "vanilla", "--data", "/d")).algorithms == Bench.Algorithms)
    assert(Bench.parseArgs(Seq("--mode", "sml1", "--data", "/d", "--runner", "gramine-sgx")).runner == "gramine-sgx")
    assert(o.runner == "native")
    intercept[IllegalArgumentException](Bench.parseArgs(Seq("--mode", "sml3", "--data", "/d")))
    intercept[IllegalArgumentException](Bench.parseArgs(Seq("--mode", "vanilla", "--data", "/d", "--algo", "svm")))
    intercept[IllegalArgumentException](Bench.parseArgs(Seq("--data", "/d")))
  }

  test("input partitions depend on the data, not on the executor cores") {
    // local[2]: Spark would pack these 4 small files into about 2 splits.
    val dir = Files.createTempDirectory("soteria-bench-splits").toString
    for ((mode, s) <- Seq("vanilla" -> None, "sml2" -> Some(session))) {
      val runner = new Runner(session.spark, s, Options(mode = mode, dataDir = dir, scale = 0.002, partitions = 4))
      for (algo <- Bench.Algorithms) {
        runner.prepare(algo)
        assert(runner.inputPartitions(algo) == 4, s"$mode/$algo")
      }
    }
  }

  test("part files are read in part-number order, whatever their sizes") {
    val spark = session.spark
    import spark.implicits._
    val dir = Files.createTempDirectory("soteria-bench-order").resolve("d").toString
    // part-00000 is the smallest file and part-00001 the largest: ordering by
    // size would put ids 10..1009 in the first partition.
    val ranges = Seq(0L until 10L, 10L until 1010L, 1010L until 1110L)
    spark.sparkContext.parallelize(ranges, ranges.size).flatMap(identity).toDF("id").write.parquet(dir)
    val firstIds = Bench.readParts(spark, dir).as[Long].rdd
      .mapPartitionsWithIndex((i, it) => Iterator(i -> it.min)).collect().sortBy(_._1).map(_._2)
    assert(firstIds.toSeq == ranges.map(_.head))
  }

  test("all algorithms run in every mode with comparable quality") {
    val dir = Files.createTempDirectory("soteria-bench").toString
    val sml1 = new SoteriaSession(session.spark, SoteriaConfig(mode = Some(SML1)))
    val runs = Seq("vanilla" -> None, "sml1" -> Some(sml1), "sml2" -> Some(session)).map { case (mode, s) =>
      val o = Options(mode = mode, dataDir = dir, scale = 0.002, partitions = 2)
      val runner = new Runner(session.spark, s, o)
      mode -> Bench.Algorithms.map { algo =>
        runner.prepare(algo)
        val m = runner.run(algo)
        assert(m.trainSeconds > 0, s"$mode/$algo")
        assert(expected(m.metric)(m.quality), s"$mode/$algo: ${m.metric} = ${m.quality}")
        val row = Bench.csvRow(o, "native", m, 0, warmup = false, (1L, 2L)).split(",")
        assert(row.length == Bench.Header.split(",").length)
        algo -> m.quality
      }.toMap
    }.toMap

    // The same data (vanilla reads it in plain, SOTERIA encrypted) gives comparable models.
    // Input splits are fixed, so sampling algorithms train the same model in SML-1 and SML-2.
    for (algo <- Seq("kmeans", "gbt", "lda"))
      assert(runs("sml1")(algo) == runs("sml2")(algo), s"$algo: sml1 ${runs("sml1")(algo)} vs sml2 ${runs("sml2")(algo)}")
    // Partitions hold the same rows in plain and encrypted data (different file
    // names and sizes), so the MLlib-based trainers match vanilla exactly.
    for (mode <- Seq("sml1", "sml2"); algo <- Seq("als", "gbt", "lda"))
      assert(runs(mode)(algo) == runs("vanilla")(algo), s"$mode/$algo: ${runs(mode)(algo)} vs vanilla ${runs("vanilla")(algo)}")
    for (mode <- Seq("sml1", "sml2"); algo <- Seq("lr", "bayes", "linear", "pca")) {
      val (v, q) = (runs("vanilla")(algo), runs(mode)(algo))
      assert(math.abs(v - q) <= 0.05 * math.max(1.0, math.abs(v)), s"$mode/$algo: $q vs vanilla $v")
    }
  }
}
