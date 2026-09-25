package soteria.bench

import java.util.Random

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Dataset, SparkSession}
import soteria.ml.SoteriaML._

/**
 * Deterministic synthetic datasets for the benchmark workloads.
 *
 * Every row is generated from (seed, row id) alone, so a dataset is the same
 * whatever the number of partitions or executors, and generation runs in
 * parallel on the executors (inside enclaves for the SOTERIA modes).
 */
object Workloads {

  /** A dataset family and its size at scale 1.0. */
  sealed abstract class Workload(val name: String, val baseRows: Long, val features: Int)
  case object Classification extends Workload("classification", 200000L, 20)
  case object Counts extends Workload("counts", 200000L, 20)
  case object Regression extends Workload("regression", 200000L, 20)
  case object Clustering extends Workload("clustering", 200000L, 10)
  case object Pca extends Workload("pca", 100000L, 50)
  case object Ratings extends Workload("ratings", 200000L, 0)
  case object Documents extends Workload("documents", 10000L, 1000)

  val MinRows = 200L
  val ClusterCount = 10
  val TopicCount = 10
  val WordsPerDocument = 50
  val LatentRank = 3

  def rows(w: Workload, scale: Double): Long = math.max(MinRows, math.round(w.baseRows * scale))

  /** Users and items of the ratings workload: 30000 x 40000 at scale 1.0, as in the original ALS runs. */
  def ratingShape(scale: Double): (Int, Int) =
    (math.max(20, math.round(30000 * scale).toInt), math.max(20, math.round(40000 * scale).toInt))

  private def rowRandom(seed: Long, id: Long): Random = new Random(seed * 0x9E3779B97F4A7C15L + id)

  private def gaussians(seed: Long, n: Int): Array[Double] = {
    val r = new Random(seed); Array.fill(n)(r.nextGaussian())
  }

  private def dot(w: Array[Double], x: Array[Double]): Double = {
    var s = 0.0; var i = 0
    while (i < w.length) { s += w(i) * x(i); i += 1 }
    s
  }

  /** Linearly separable labels with 5% label noise (logistic regression). */
  def classification(spark: SparkSession, n: Long, d: Int, parts: Int, seed: Long): Dataset[ClassificationData] = {
    import spark.implicits._
    val w = gaussians(seed, d)
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val x = Array.fill(d)(r.nextGaussian())
      val label = if (dot(w, x) > 0) 1.0 else 0.0
      ClassificationData(Vectors.dense(x), if (r.nextDouble() < 0.05) 1.0 - label else label)
    }
  }

  /** Non-negative term counts whose rates depend on the class (multinomial naive Bayes). */
  def counts(spark: SparkSession, n: Long, d: Int, parts: Int, seed: Long): Dataset[ClassificationData] = {
    import spark.implicits._
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val label = if (r.nextBoolean()) 1.0 else 0.0
      val x = Array.tabulate(d) { j =>
        val rate = if ((j < d / 2) == (label == 1.0)) 4.0 else 1.0
        math.floor(r.nextDouble() * 2 * rate)
      }
      ClassificationData(Vectors.dense(x), label)
    }
  }

  /** y = w.x + 1 + noise (linear regression, GBT). */
  def regression(spark: SparkSession, n: Long, d: Int, parts: Int, seed: Long): Dataset[RegressionData] = {
    import spark.implicits._
    val w = gaussians(seed, d)
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val x = Array.fill(d)(r.nextGaussian())
      RegressionData(Vectors.dense(x), dot(w, x) + 1.0 + 0.1 * r.nextGaussian())
    }
  }

  /** Gaussian blobs around ClusterCount centers (K-Means). */
  def clustering(spark: SparkSession, n: Long, d: Int, parts: Int, seed: Long): Dataset[ClusteringData] = {
    import spark.implicits._
    val centers = gaussians(seed, ClusterCount * d).map(_ * 5)
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val c = (id % ClusterCount).toInt
      ClusteringData(Vectors.dense(Array.tabulate(d)(j => centers(c * d + j) + r.nextGaussian())), id)
    }
  }

  /** Correlated features with decaying variance (PCA). */
  def pca(spark: SparkSession, n: Long, d: Int, parts: Int, seed: Long): Dataset[PCAData] = {
    import spark.implicits._
    val mix = gaussians(seed, d * d)
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val z = Array.tabulate(d)(j => r.nextGaussian() / (1 + j))
      PCAData(Vectors.dense(Array.tabulate(d)(i => z(i) + 0.1 * dot(mix.slice(i * d, (i + 1) * d), z))), id)
    }
  }

  /** Ratings in [1, 5] from rank-LatentRank user and item factors (ALS). */
  def ratings(spark: SparkSession, n: Long, users: Int, items: Int, parts: Int, seed: Long): Dataset[RecommendationData] = {
    import spark.implicits._
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val u = r.nextInt(users); val i = r.nextInt(items)
      val score = 3.0 + dot(gaussians(seed + 1 + u, LatentRank), gaussians(seed - 1 - i, LatentRank)) + 0.1 * r.nextGaussian()
      RecommendationData(u, i, math.max(1.0, math.min(5.0, score)).toFloat)
    }
  }

  /** Bags of words, each document drawn mostly from one topic's slice of the vocabulary (LDA). */
  def documents(spark: SparkSession, n: Long, vocab: Int, parts: Int, seed: Long): Dataset[LDAData] = {
    import spark.implicits._
    val slice = vocab / TopicCount
    spark.range(0, n, 1, parts).map { id =>
      val r = rowRandom(seed, id)
      val topic = (id % TopicCount).toInt
      val counts = new Array[Double](vocab)
      for (_ <- 0 until WordsPerDocument) {
        val word = if (r.nextDouble() < 0.8) topic * slice + r.nextInt(slice) else r.nextInt(vocab)
        counts(word) += 1
      }
      LDAData(Vectors.dense(counts).toSparse, id)
    }
  }
}
