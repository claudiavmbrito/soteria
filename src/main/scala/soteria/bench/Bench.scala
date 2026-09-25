package soteria.bench

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.clustering.{KMeans, LDA, LDAModel}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SaveMode, SparkSession}
import soteria.core.SoteriaCore
import soteria.core.SoteriaCore.{EncryptedDataset, SoteriaConfig, SoteriaSession}
import soteria.ml.ExtendedSoteriaML.{SoteriaALS, SoteriaGBT, SoteriaLDA}
import soteria.ml.SoteriaML._
import soteria.partition.{ComputationPartitioner, SoteriaMode}

/**
 * Benchmark of the eight paper workloads on vanilla Spark (plain Parquet,
 * MLlib) and on SOTERIA (encrypted Parquet, SOTERIA trainers) in SML-1 or
 * SML-2. Both sides use the same data and hyperparameters.
 *
 * Prints one CSV row per repetition, prefixed with `RESULT,` (the header is
 * prefixed with `HEADER,`), so a driver running inside an enclave needs no
 * host file for its results.
 *
 * Usage: Bench --mode vanilla|sml1|sml2 --data DIR [--algo all|lr,kmeans,...]
 *              [--scale 1.0] [--partitions 8] [--reps 3] [--warmup 1] [--seed 42]
 */
object Bench {

  val Algorithms: Seq[String] = Seq("als", "bayes", "gbt", "kmeans", "lda", "linear", "lr", "pca")

  val Header: String =
    "timestamp,runner,mode,algo,scale,rows,partitions,rep,warmup,train_s,metric,quality,enclave_tasks,untrusted_tasks,input_partitions"

  // Hyperparameters shared by the vanilla and SOTERIA runs.
  val Iterations = 20
  val Tolerance = 1e-6
  val KMeansTolerance = 1e-4
  val TreeIterations = 10
  val FactorIterations = 10
  val Rank = 10
  val Components = 10

  case class Options(
    algorithms: Seq[String] = Algorithms,
    mode: String = "",
    dataDir: String = "",
    scale: Double = 1.0,
    partitions: Int = 8,
    reps: Int = 3,
    warmup: Int = 1,
    seed: Long = 42L,
    ephemeralKey: Boolean = false
  ) {
    def vanilla: Boolean = mode == "vanilla"
  }

  case class Measurement(algorithm: String, rows: Long, trainSeconds: Double, metric: String, quality: Double,
    inputPartitions: Int)

  def parseArgs(args: Seq[String]): Options = {
    def loop(rest: List[String], o: Options): Options = rest match {
      case Nil => o
      case "--algo" :: v :: t =>
        val algos = if (v == "all") Algorithms else v.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSeq
        algos.filterNot(Algorithms.contains).foreach(a => usage(s"unknown algorithm '$a' (${Algorithms.mkString(", ")})"))
        loop(t, o.copy(algorithms = algos))
      case "--mode" :: v :: t =>
        val m = v.trim.toLowerCase
        if (m != "vanilla") SoteriaMode.parse(m)
        loop(t, o.copy(mode = if (m == "vanilla") m else m.replace("-", "")))
      case "--data" :: v :: t => loop(t, o.copy(dataDir = v))
      case "--scale" :: v :: t => loop(t, o.copy(scale = v.toDouble))
      case "--partitions" :: v :: t => loop(t, o.copy(partitions = v.toInt))
      case "--reps" :: v :: t => loop(t, o.copy(reps = v.toInt))
      case "--warmup" :: v :: t => loop(t, o.copy(warmup = v.toInt))
      case "--seed" :: v :: t => loop(t, o.copy(seed = v.toLong))
      case "--ephemeral-key" :: t => loop(t, o.copy(ephemeralKey = true))
      case other :: _ => usage(s"unknown or incomplete option '$other'")
    }
    val o = loop(args.toList, Options())
    if (o.mode.isEmpty) usage("--mode is required")
    if (o.dataDir.isEmpty) usage("--data is required")
    require(o.scale > 0 && o.partitions > 0 && o.reps > 0 && o.warmup >= 0, s"invalid options: $o")
    o
  }

  private def usage(msg: String): Nothing = throw new IllegalArgumentException(
    s"$msg\nusage: Bench --mode vanilla|sml1|sml2 --data DIR [--algo all|${Algorithms.mkString(",")}] " +
      "[--scale 1.0] [--partitions 8] [--reps 3] [--warmup 1] [--seed 42] [--ephemeral-key]")

  def csvRow(o: Options, runner: String, m: Measurement, rep: Int, warmup: Boolean, placement: (Long, Long)): String =
    Seq(Instant.now().toString, runner, o.mode, m.algorithm, o.scale, m.rows, o.partitions, rep, warmup,
      f"${m.trainSeconds}%.3f", m.metric, f"${m.quality}%.6f", placement._1, placement._2, m.inputPartitions).mkString(",")

  def main(args: Array[String]): Unit = {
    val o = parseArgs(args.toSeq)
    val soteria =
      if (o.vanilla) None
      else Some(SoteriaCore.createSession(s"soteria-bench-${o.mode}",
        SoteriaConfig(mode = Some(SoteriaMode.parse(o.mode)), requireProvisionedKey = !o.ephemeralKey)))
    val spark = soteria.map(_.spark).getOrElse(SparkSession.builder().appName("soteria-bench-vanilla").getOrCreate())
    val placement = new PlacementCounter
    spark.sparkContext.addSparkListener(placement)
    val runner = new Runner(spark, soteria, o)
    val runnerName = sys.env.getOrElse("SOTERIA_RUNNER", "native")

    println(s"HEADER,$Header")
    val failed = o.algorithms.filterNot { algo =>
      try {
        runner.prepare(algo)
        for (rep <- 0 until o.warmup + o.reps) {
          val before = placement.snapshot
          val m = runner.run(algo)
          Thread.sleep(1000) // let the listener bus deliver the last task events
          val after = placement.snapshot
          val row = csvRow(o, runnerName, m, rep, rep < o.warmup, (after._1 - before._1, after._2 - before._2))
          println(s"RESULT,$row")
        }
        true
      } catch {
        case NonFatal(e) =>
          println(s"ERROR: $algo failed: $e")
          e.printStackTrace()
          false
      }
    }
    soteria.map(_.close()).getOrElse(spark.stop())
    if (failed.nonEmpty) {
      println(s"FAIL: ${failed.mkString(", ")}")
      sys.exit(1)
    }
    println(s"DONE: ${o.algorithms.mkString(", ")} in mode ${o.mode}")
  }

  /** Counts finished tasks by where they ran: executors with or without the enclave resource. */
  class PlacementCounter extends SparkListener {
    private val enclaveExecutors = ConcurrentHashMap.newKeySet[String]()
    private val enclaveTasks = new AtomicLong
    private val untrustedTasks = new AtomicLong

    override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit =
      if (e.executorInfo.resourcesInfo.contains(ComputationPartitioner.EnclaveResource)) enclaveExecutors.add(e.executorId)

    override def onTaskEnd(t: SparkListenerTaskEnd): Unit =
      if (enclaveExecutors.contains(t.taskInfo.executorId)) enclaveTasks.incrementAndGet()
      else untrustedTasks.incrementAndGet()

    def snapshot: (Long, Long) = (enclaveTasks.get(), untrustedTasks.get())
  }

  /** Generates the datasets and runs one training per call. */
  class Runner(spark: SparkSession, soteria: Option[SoteriaSession], o: Options) {
    import spark.implicits._
    import Workloads._

    // Split every dataset into the same input partitions in every mode. By
    // default Spark sizes file splits from the executor cores registered at
    // read time, which differ between vanilla, SML-1 and SML-2 (untrusted
    // executors); algorithms that sample per partition (K-Means seeding, GBT
    // binning, LDA minibatches) would then train different models.
    spark.conf.set("spark.sql.files.minPartitionNum", o.partitions.toLong)

    private def workload(algo: String): Workload = algo match {
      case "lr" => Classification
      case "bayes" => Counts
      case "linear" | "gbt" => Regression
      case "kmeans" => Clustering
      case "pca" => Pca
      case "als" => Ratings
      case "lda" => Documents
    }

    private def path(w: Workload): String =
      s"${o.dataDir}/scale-${o.scale}-seed-${o.seed}-parts-${o.partitions}/${w.name}.${if (o.vanilla) "plain" else "enc"}"

    /** Writes the algorithm's dataset unless a complete copy already exists. */
    def prepare(algo: String): Unit = {
      val w = workload(algo)
      val p = new Path(path(w), "_SUCCESS")
      if (p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)) return
      val n = rows(w, o.scale)
      val (parts, seed) = (o.partitions, o.seed)
      val data: Dataset[_] = w match {
        case Classification => classification(spark, n, w.features, parts, seed)
        case Counts => counts(spark, n, w.features, parts, seed)
        case Regression => regression(spark, n, w.features, parts, seed)
        case Clustering => clustering(spark, n, w.features, parts, seed)
        case Pca => pca(spark, n, w.features, parts, seed)
        case Ratings => val (u, i) = ratingShape(o.scale); ratings(spark, n, u, i, parts, seed)
        case Documents => documents(spark, n, w.features, parts, seed)
      }
      soteria match {
        case Some(s) => s.saveEncrypted(data, path(w), SaveMode.Overwrite)
        case None => data.write.mode(SaveMode.Overwrite).parquet(path(w))
      }
    }

    private def plain(algo: String): DataFrame = spark.read.parquet(path(workload(algo)))

    private def encrypted[T: Encoder](algo: String): EncryptedDataset[T] =
      soteria.get.loadEncryptedDataset[T](path(workload(algo)))

    private def timed[M](train: => M): (M, Double) = {
      val start = System.nanoTime()
      val model = train
      (model, (System.nanoTime() - start) / 1e9)
    }

    def run(algo: String): Measurement = {
      val n = rows(workload(algo), o.scale)
      val (metric, (quality, seconds)) = algo match {
        case "lr" => "accuracy" -> (soteria match {
          case None =>
            val df = plain(algo)
            val (m, t) = timed(new LogisticRegression().setMaxIter(Iterations).setTol(Tolerance).fit(df))
            (accuracy(m.transform(df)), t)
          case Some(s) =>
            val ds = encrypted[ClassificationData](algo)
            val (m, t) = timed(new SoteriaLogisticRegression(s, maxIterations = Iterations, tol = Tolerance).train(ds))
            (Bench.accuracy(ds.data, m.predict), t)
        })
        case "bayes" => "accuracy" -> (soteria match {
          case None =>
            val df = plain(algo)
            val (m, t) = timed(new NaiveBayes().setModelType("multinomial").setSmoothing(1.0).fit(df))
            (accuracy(m.transform(df)), t)
          case Some(s) =>
            val ds = encrypted[ClassificationData](algo)
            val (m, t) = timed(new SoteriaNaiveBayes(s, smoothing = 1.0).train(ds))
            (Bench.accuracy(ds.data, m.predict), t)
        })
        case "linear" => "rmse" -> (soteria match {
          case None =>
            val df = plain(algo)
            val (m, t) = timed(new LinearRegression().setSolver("normal").fit(df))
            (rmse(m.transform(df), "label"), t)
          case Some(s) =>
            val ds = encrypted[RegressionData](algo)
            val (m, t) = timed(new SoteriaLinearRegression(s).train(ds))
            (Bench.rmse(ds.data, m.predict), t)
        })
        case "gbt" => "rmse" -> {
          val (m, t) = soteria match {
            case None => timed(new GBTRegressor().setMaxIter(TreeIterations).setSeed(o.seed).fit(plain(algo)))
            case Some(s) => timed(new SoteriaGBT(s, maxIter = TreeIterations, seed = o.seed).train(encrypted[RegressionData](algo)))
          }
          (rmse(m.transform(dataFrame(algo)), "label"), t)
        }
        case "kmeans" => "cost" -> (soteria match {
          case None =>
            val (m, t) = timed(new KMeans().setK(ClusterCount).setMaxIter(Iterations).setTol(KMeansTolerance)
              .setSeed(o.seed).fit(plain(algo)))
            (m.summary.trainingCost, t)
          case Some(s) =>
            val ds = encrypted[ClusteringData](algo)
            val (m, t) = timed(new SoteriaKMeans(s, ClusterCount, Iterations, KMeansTolerance, o.seed).train(ds))
            (Bench.kmeansCost(ds.data, m.centroids), t)
        })
        case "pca" => "explained_variance" -> (soteria match {
          case None =>
            val (m, t) = timed(new PCA().setK(Components).setInputCol("features").fit(plain(algo)))
            (m.explainedVariance.toArray.sum, t)
          case Some(s) =>
            val (m, t) = timed(new SoteriaPCA(s, Components).train(encrypted[PCAData](algo)))
            (m.explainedVariance.toArray.sum, t)
        })
        case "als" => "rmse" -> {
          val (m, t) = soteria match {
            case None => timed(new ALS().setRank(Rank).setMaxIter(FactorIterations).setRegParam(0.1).setSeed(o.seed)
              .setUserCol("user").setItemCol("item").setRatingCol("rating").setColdStartStrategy("drop").fit(plain(algo)))
            case Some(s) => timed(new SoteriaALS(s, rank = Rank, maxIterations = FactorIterations, regParam = 0.1, seed = o.seed)
              .train(encrypted[RecommendationData](algo)))
          }
          (rmse(m.transform(dataFrame(algo)), "rating"), t)
        }
        case "lda" => "log_perplexity" -> {
          val (m, t) = soteria match {
            case None => timed[LDAModel](new LDA().setK(TopicCount).setMaxIter(FactorIterations).setSeed(o.seed).fit(plain(algo)))
            case Some(s) => timed[LDAModel](new SoteriaLDA(s, TopicCount, maxIter = FactorIterations, seed = o.seed)
              .train(encrypted[LDAData](algo)))
          }
          (m.logPerplexity(dataFrame(algo)), t)
        }
      }
      Measurement(algo, n, seconds, metric, quality, inputPartitions(algo))
    }

    /** Number of partitions the algorithm's dataset is read into. */
    def inputPartitions(algo: String): Int = dataFrame(algo).rdd.getNumPartitions

    /** The algorithm's dataset as a DataFrame, decrypted in the SOTERIA modes. */
    private def dataFrame(algo: String): DataFrame =
      if (soteria.isEmpty) plain(algo) else soteria.get.spark.read.parquet(path(workload(algo)))

    private def accuracy(predictions: DataFrame): Double =
      new MulticlassClassificationEvaluator().setMetricName("accuracy").evaluate(predictions)

    private def rmse(predictions: DataFrame, label: String): Double =
      new RegressionEvaluator().setLabelCol(label).setMetricName("rmse").evaluate(predictions)
  }

  // Evaluation of the SOTERIA models: plain functions, so closures capture only the model.

  def accuracy(data: Dataset[ClassificationData], predict: Vector => Double): Double =
    data.rdd.map(p => if (predict(p.features) == p.label) 1.0 else 0.0).mean()

  def rmse(data: Dataset[RegressionData], predict: Vector => Double): Double =
    math.sqrt(data.rdd.map { p => val e = predict(p.features) - p.label; e * e }.mean())

  def kmeansCost(data: Dataset[ClusteringData], centroids: Array[Vector]): Double =
    data.rdd.map(p => centroids.map(c => Vectors.sqdist(p.features, c)).min).sum()
}
