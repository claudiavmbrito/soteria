package soteria.ml

import scala.collection.mutable

import breeze.linalg.{eigSym, DenseMatrix => BDM, DenseVector => BDV}
import breeze.optimize.{DiffFunction, LBFGS}
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import soteria.core.SoteriaCore._

/**
 * SML-2 partitioned trainers (Section 4 of the paper; `docs/REBUILD_PLAN.md`).
 *
 * Each trainer touches records only inside enclave stages. What leaves the
 * enclave, through [[SoteriaSession.statistic]], is a per-partition statistic:
 * gradient and loss sums, sufficient statistics or counts. The driver, which
 * runs inside an enclave, turns the combined statistic into a model update.
 */
object SoteriaML {

  // Data structures for different ML tasks
  case class TrainingData(features: Vector, label: Double)
  case class ClusteringData(features: Vector, id: Long)
  case class RecommendationData(user: Int, item: Int, rating: Float)
  case class ClassificationData(features: Vector, label: Double)
  case class RegressionData(features: Vector, label: Double)
  case class LDAData(features: Vector, docId: Long)
  case class PCAData(features: Vector, id: Long)

  private def numFeatures(session: SoteriaSession, rdd: RDD[Vector]): Int = {
    val d = session.statistic(rdd)(-1)((acc, v) => math.max(acc, v.size), math.max)
    require(d > 0, "empty training set")
    d
  }

  // ---------------------------------------------------------------- K-Means

  /**
   * Lloyd's algorithm with k-means++ seeding. Per iteration, untrusted
   * executors see per-partition (sum, count) per cluster and the cost.
   */
  class SoteriaKMeans(session: SoteriaSession, k: Int, maxIterations: Int = 100, tol: Double = 1e-4, seed: Long = 42L) {

    def train(dataset: EncryptedDataset[ClusteringData]): SoteriaKMeansModel = {
      val points = dataset.data.rdd.map(_.features).cache()
      try {
        var centroids = SoteriaKMeans.selectInitialCentroids(points, k, seed)
        val d = centroids.head.size
        var iteration = 0
        var converged = false

        while (iteration < maxIterations && !converged) {
          val current = centroids
          // stats layout: k blocks of (d sums, count), then total cost
          val stats = session.statistic(points)(new Array[Double](k * (d + 1) + 1))(
            (acc, p) => {
              val c = SoteriaKMeans.findClosestCentroid(p, current)
              val base = c * (d + 1)
              p.foreachActive((j, v) => acc(base + j) += v)
              acc(base + d) += 1
              acc(acc.length - 1) += Vectors.sqdist(p, current(c))
              acc
            },
            SoteriaML.addInPlace)

          val updated = Array.tabulate(k) { c =>
            val base = c * (d + 1)
            val count = stats(base + d)
            if (count > 0) Vectors.dense(Array.tabulate(d)(j => stats(base + j) / count)) else current(c)
          }
          converged = current.zip(updated).forall { case (a, b) => math.sqrt(Vectors.sqdist(a, b)) < tol }
          centroids = updated
          iteration += 1
        }
        SoteriaKMeansModel(centroids, k)
      } finally points.unpersist()
    }
  }

  object SoteriaKMeans {
    private val InitSampleSize = 10000

    /** k-means++ seeding over a bounded random sample, computed on the driver (inside the enclave). */
    private[ml] def selectInitialCentroids(points: RDD[Vector], k: Int, seed: Long): Array[Vector] = {
      val sample = points.takeSample(withReplacement = false, InitSampleSize, seed)
      require(sample.length >= k, s"need at least $k points, got ${sample.length}")
      val rnd = new scala.util.Random(seed)
      val centroids = mutable.ArrayBuffer(sample(rnd.nextInt(sample.length)))
      val dist = sample.map(p => Vectors.sqdist(p, centroids.head))
      while (centroids.length < k) {
        // Pick the next centroid with probability proportional to squared distance.
        var target = rnd.nextDouble() * dist.sum
        var idx = 0
        while (idx < dist.length - 1 && target >= dist(idx)) { target -= dist(idx); idx += 1 }
        val next = sample(idx)
        centroids += next
        for (i <- sample.indices) dist(i) = math.min(dist(i), Vectors.sqdist(sample(i), next))
      }
      centroids.toArray
    }

    private[ml] def findClosestCentroid(point: Vector, centroids: Array[Vector]): Int = {
      var best = 0
      var bestDist = Double.PositiveInfinity
      var i = 0
      while (i < centroids.length) {
        val dist = Vectors.sqdist(point, centroids(i))
        if (dist < bestDist) { bestDist = dist; best = i }
        i += 1
      }
      best
    }
  }

  case class SoteriaKMeansModel(centroids: Array[Vector], k: Int) {
    def predict(point: Vector): Int = SoteriaKMeans.findClosestCentroid(point, centroids)

    def cost(points: Seq[Vector]): Double = points.map(p => Vectors.sqdist(p, centroids(predict(p)))).sum
  }

  // ---------------------------------------------------- Logistic regression

  /**
   * Binary logistic regression (labels 0/1) trained with L-BFGS on the driver.
   * Minimizes mean log-loss + regParam / 2 * ||w||^2 (intercept not
   * regularized). Per iteration, untrusted executors see per-partition sums
   * of the loss and gradient.
   */
  class SoteriaLogisticRegression(
    session: SoteriaSession,
    maxIterations: Int = 100,
    regParam: Double = 0.0,
    tol: Double = 1e-6
  ) {

    def train(dataset: EncryptedDataset[ClassificationData]): SoteriaLogisticRegressionModel = {
      val data = dataset.data.rdd.cache()
      try {
        val d = numFeatures(session, data.map(_.features))

        val objective = new DiffFunction[BDV[Double]] {
          override def calculate(x: BDV[Double]): (Double, BDV[Double]) = {
            val coef = x.toArray
            val stats = SoteriaLogisticRegression.lossAndGradientSums(session, data, coef)
            val n = stats(d + 2)
            val grad = BDV.tabulate(d + 1)(j => stats(j) / n + (if (j < d) regParam * coef(j) else 0.0))
            var reg = 0.0
            for (j <- 0 until d) reg += coef(j) * coef(j)
            (stats(d + 1) / n + regParam / 2 * reg, grad)
          }
        }

        val optimum = new LBFGS[BDV[Double]](maxIter = maxIterations, m = 10, tolerance = tol)
          .minimize(objective, BDV.zeros[Double](d + 1))
        SoteriaLogisticRegressionModel(Vectors.dense(optimum.toArray.take(d)), optimum(d))
      } finally data.unpersist()
    }
  }

  object SoteriaLogisticRegression {
    /**
     * Returns d gradient sums, the intercept gradient sum, the loss sum and the
     * count. Kept outside the DiffFunction so task closures capture only `coef`.
     */
    private[ml] def lossAndGradientSums(session: SoteriaSession, data: RDD[ClassificationData], coef: Array[Double]): Array[Double] = {
      val d = coef.length - 1
      session.statistic(data)(new Array[Double](d + 3))(
        (acc, p) => {
          val m = margin(coef, p.features)
          val mult = sigmoid(m) - p.label
          p.features.foreachActive((j, v) => acc(j) += mult * v)
          acc(d) += mult
          acc(d + 1) += softplus(m) - p.label * m
          acc(d + 2) += 1
          acc
        },
        SoteriaML.addInPlace)
    }
    
    private[ml] def sigmoid(x: Double): Double = 1.0 / (1.0 + math.exp(-x))

    /** log(1 + e^x), computed stably. */
    private[ml] def softplus(x: Double): Double = if (x > 0) x + math.log1p(math.exp(-x)) else math.log1p(math.exp(x))

    /** coef has d weights followed by the intercept. */
    private[ml] def margin(coef: Array[Double], features: Vector): Double = {
      var s = coef(coef.length - 1)
      features.foreachActive((j, v) => s += coef(j) * v)
      s
    }
  }

  case class SoteriaLogisticRegressionModel(weights: Vector, intercept: Double) {
    def probability(features: Vector): Double =
      SoteriaLogisticRegression.sigmoid(SoteriaLogisticRegression.margin(weights.toArray :+ intercept, features))

    def predict(features: Vector): Double = if (probability(features) >= 0.5) 1.0 else 0.0
  }

  // ------------------------------------------------------ Linear regression

  /**
   * Least squares with an intercept, solved exactly from the normal
   * equations. Minimizes 1/(2n) * ||y - Xw - b||^2 + regParam / 2 * ||w||^2.
   * Untrusted executors see per-partition sums of x, y, x x^T and x y.
   */
  class SoteriaLinearRegression(session: SoteriaSession, regParam: Double = 0.0) {

    def train(dataset: EncryptedDataset[RegressionData]): SoteriaLinearRegressionModel = {
      val data = dataset.data.rdd
      val d = numFeatures(session, data.map(_.features))
      // stats layout: n, sum x (d), sum y, sum x x^T (d*d, row-major), sum x y (d)
      val xOff = 1; val yOff = 1 + d; val xxOff = 2 + d; val xyOff = 2 + d + d * d
      val s = session.statistic(data)(new Array[Double](2 + 2 * d + d * d))(
        (acc, p) => {
          val x = p.features.toArray
          acc(0) += 1
          var i = 0
          while (i < d) {
            acc(xOff + i) += x(i)
            acc(xyOff + i) += x(i) * p.label
            var j = 0
            while (j < d) { acc(xxOff + i * d + j) += x(i) * x(j); j += 1 }
            i += 1
          }
          acc(yOff) += p.label
          acc
        },
        SoteriaML.addInPlace)

      val n = s(0)
      val mu = BDV.tabulate(d)(i => s(xOff + i) / n)
      val yMean = s(yOff) / n
      val a = BDM.tabulate(d, d)((i, j) => s(xxOff + i * d + j) / n - mu(i) * mu(j) + (if (i == j) regParam else 0.0))
      val b = BDV.tabulate(d)(i => s(xyOff + i) / n - mu(i) * yMean)
      val w = a \ b
      SoteriaLinearRegressionModel(Vectors.dense(w.toArray), yMean - (w dot mu))
    }
  }

  case class SoteriaLinearRegressionModel(coefficients: Vector, intercept: Double) {
    def predict(features: Vector): Double = {
      var s = intercept
      features.foreachActive((j, v) => s += coefficients(j) * v)
      s
    }
  }

  // ------------------------------------------------------------ Naive Bayes

  /**
   * Multinomial naive Bayes with additive smoothing (as MLlib's default).
   * Labels must be class indices 0, 1, ...; features must be non-negative.
   * Untrusted executors see per-partition per-class counts and feature sums.
   */
  class SoteriaNaiveBayes(session: SoteriaSession, smoothing: Double = 1.0) {

    def train(dataset: EncryptedDataset[ClassificationData]): SoteriaNaiveBayesModel = {
      // per class: (count, feature sums)
      val stats = session.statistic(dataset.data.rdd)(mutable.HashMap.empty[Int, Array[Double]])(
        (acc, p) => {
          require(p.label >= 0 && p.label == math.floor(p.label), s"label ${p.label} is not a class index")
          val x = p.features.toArray
          require(x.forall(_ >= 0), "multinomial naive Bayes needs non-negative features")
          val a = acc.getOrElseUpdate(p.label.toInt, new Array[Double](x.length + 1))
          a(0) += 1
          var j = 0
          while (j < x.length) { a(j + 1) += x(j); j += 1 }
          acc
        },
        (m1, m2) => {
          m2.foreach { case (c, a) => m1.get(c) match {
            case Some(b) => SoteriaML.addInPlace(b, a)
            case None => m1(c) = a
          } }
          m1
        })
      require(stats.nonEmpty, "empty training set")

      val numClasses = stats.keys.max + 1
      val d = stats.values.head.length - 1
      val n = stats.values.map(_(0)).sum
      val piLogDenom = math.log(n + numClasses * smoothing)
      val pi = Array.tabulate(numClasses)(c => math.log(stats.get(c).map(_(0)).getOrElse(0.0) + smoothing) - piLogDenom)
      val theta = Array.ofDim[Double](numClasses, d)
      for (c <- 0 until numClasses) {
        val sums = stats.get(c).map(_.drop(1)).getOrElse(new Array[Double](d))
        val thetaLogDenom = math.log(sums.sum + d * smoothing)
        for (j <- 0 until d) theta(c)(j) = math.log(sums(j) + smoothing) - thetaLogDenom
      }
      SoteriaNaiveBayesModel(
        new DenseVector(pi),
        new DenseMatrix(numClasses, d, Array.tabulate(numClasses * d)(i => theta(i % numClasses)(i / numClasses))))
    }
  }

  /** `pi` holds log class priors; `theta(c, j)` log feature probabilities. */
  case class SoteriaNaiveBayesModel(pi: DenseVector, theta: DenseMatrix) {
    def numClasses: Int = pi.size

    def predict(features: Vector): Double = {
      val scores = Array.tabulate(numClasses) { c =>
        var s = pi(c)
        features.foreachActive((j, v) => s += theta(c, j) * v)
        s
      }
      scores.indices.maxBy(scores).toDouble
    }
  }

  // -------------------------------------------------------------------- PCA

  /**
   * Principal components of the sample covariance. Untrusted executors see
   * per-partition sums of x and x x^T; the eigendecomposition runs on the
   * driver.
   */
  class SoteriaPCA(session: SoteriaSession, k: Int) {

    def train(dataset: EncryptedDataset[PCAData]): SoteriaPCAModel = {
      val data = dataset.data.rdd.map(_.features)
      val d = numFeatures(session, data)
      require(k <= d, s"k = $k exceeds the number of features $d")
      val s = session.statistic(data)(new Array[Double](1 + d + d * d))(
        (acc, v) => {
          val x = v.toArray
          acc(0) += 1
          var i = 0
          while (i < d) {
            acc(1 + i) += x(i)
            var j = 0
            while (j < d) { acc(1 + d + i * d + j) += x(i) * x(j); j += 1 }
            i += 1
          }
          acc
        },
        SoteriaML.addInPlace)

      val n = s(0)
      require(n > 1, "PCA needs at least two rows")
      val mu = Array.tabulate(d)(i => s(1 + i) / n)
      val cov = BDM.tabulate(d, d)((i, j) => (s(1 + d + i * d + j) - n * mu(i) * mu(j)) / (n - 1))
      val eig = eigSym(cov)
      val order = (0 until d).sortBy(i => -eig.eigenvalues(i)).take(k)
      val total = breeze.linalg.sum(eig.eigenvalues)
      val pc = new DenseMatrix(d, k, order.flatMap(i => eig.eigenvectors(::, i).toArray).toArray)
      SoteriaPCAModel(pc, new DenseVector(order.map(i => eig.eigenvalues(i) / total).toArray))
    }
  }

  /** `pc` is d x k (components as columns); vectors are projected without centering, as in MLlib. */
  case class SoteriaPCAModel(pc: DenseMatrix, explainedVariance: DenseVector) {
    def transform(features: Vector): Vector = pc.transpose.multiply(features)
  }

  private[ml] def addInPlace(a: Array[Double], b: Array[Double]): Array[Double] = {
    var i = 0
    while (i < a.length) { a(i) += b(i); i += 1 }
    a
  }
}
