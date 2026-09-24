package soteria.ml

import soteria.core.SoteriaCore._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier}
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.Random

/**
 * SOTERIA Machine Learning Implementation
 * Based on the IEEE paper "Privacy-Preserving Machine Learning on Apache Spark"
 * ML algorithms expressed through SoteriaSession.executeWithPartitioning.
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
  
  /**
   * SOTERIA K-Means Clustering
   */
  class SoteriaKMeans(session: SoteriaSession, k: Int, maxIterations: Int = 100, seed: Long = 42L) {
    
    def train(dataset: EncryptedDataset[ClusteringData]): SoteriaKMeansModel = {
      // Phase 1: data preprocessing and initial centroid selection (sensitive)
      val (dataRDD, initialCentroids) = session.executeWithPartitioning(
        dataset,
        "data_preprocessing",
        (data: Dataset[ClusteringData]) => {
          val dataRDD = data.rdd.map(row => (row.id, row.features)).cache()
          (dataRDD, SoteriaKMeans.selectInitialCentroids(dataRDD, k, seed))
        }
      )
      
      // Phase 2: iterative clustering with computation partitioning
      val finalCentroids = performIterativeClustering(dataset, dataRDD, initialCentroids)
      dataRDD.unpersist()
      
      SoteriaKMeansModel(finalCentroids, k)
    }
    
    private def performIterativeClustering(
      dataset: EncryptedDataset[ClusteringData],
      dataRDD: RDD[(Long, Vector)],
      initialCentroids: Array[Vector]
    ): Array[Vector] = {
      
      var centroids = initialCentroids
      var iteration = 0
      var converged = false
      
      while (iteration < maxIterations && !converged) {
        val current = centroids
        // Point-to-centroid assignment
        val assignments = dataRDD.map { case (_, point) =>
          (SoteriaKMeans.findClosestCentroid(point, current), (point.toArray, 1L))
        }
        
        // Centroid update (sensitive operation)
        val newCentroids = session.executeWithPartitioning(
          dataset,
          "model_update",
          (_: Dataset[ClusteringData]) => {
            val centroidUpdates = assignments.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
              (sum1.zip(sum2).map { case (a, b) => a + b }, count1 + count2)
            }.collectAsMap()
            
            current.indices.map { i =>
              centroidUpdates.get(i) match {
                case Some((sum, count)) => Vectors.dense(sum.map(_ / count))
                case None => current(i)
              }
            }.toArray
          }
        )
        
        val maxDelta = current.zip(newCentroids).map { case (old, newC) =>
          Vectors.sqdist(old, newC)
        }.max
        
        converged = maxDelta < 1e-6
        centroids = newCentroids
        iteration += 1
      }
      
      centroids
    }
  }
  
  object SoteriaKMeans {
    private val InitSampleSize = 10000
    
    /** k-means++ seeding over a bounded random sample of the data. */
    private[ml] def selectInitialCentroids(dataRDD: RDD[(Long, Vector)], k: Int, seed: Long): Array[Vector] = {
      val sample = dataRDD.takeSample(withReplacement = false, InitSampleSize, seed).map(_._2)
      require(sample.length >= k, s"need at least $k points, got ${sample.length}")
      val rnd = new Random(seed)
      val centroids = scala.collection.mutable.ArrayBuffer(sample(rnd.nextInt(sample.length)))
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
      centroids.indices.minBy(i => Vectors.sqdist(point, centroids(i)))
    }
  }
  
  case class SoteriaKMeansModel(centroids: Array[Vector], k: Int) {
    def predict(point: Vector): Int = SoteriaKMeans.findClosestCentroid(point, centroids)
  }
  
  /**
   * SOTERIA Logistic Regression (full-batch gradient descent)
   */
  class SoteriaLogisticRegression(session: SoteriaSession, maxIterations: Int = 100, stepSize: Double = 0.01) {
    
    def train(dataset: EncryptedDataset[ClassificationData]): SoteriaLogisticRegressionModel = {
      val (weights, intercept) = session.executeWithPartitioning(
        dataset,
        "gradient_computation",
        (data: Dataset[ClassificationData]) => {
          val trainingRDD = data.rdd.cache()
          val numExamples = trainingRDD.count().toDouble
          require(numExamples > 0, "empty training set")
          
          val numFeatures = trainingRDD.first().features.size
          var weights = Array.fill(numFeatures)(0.0)
          var intercept = 0.0
          
          for (_ <- 1 to maxIterations) {
            val (gradSum, errSum) = SoteriaLogisticRegression.computeGradient(trainingRDD, weights, intercept)
            weights = weights.zip(gradSum).map { case (w, g) => w - stepSize * g / numExamples }
            intercept -= stepSize * errSum / numExamples
          }
          
          trainingRDD.unpersist()
          (Vectors.dense(weights), intercept)
        }
      )
      
      SoteriaLogisticRegressionModel(weights, intercept)
    }
  }
  
  object SoteriaLogisticRegression {
    private[ml] def sigmoid(x: Double): Double = 1.0 / (1.0 + math.exp(-x))
    
    private[ml] def margin(weights: Array[Double], features: Vector, intercept: Double): Double = {
      val f = features.toArray
      var s = intercept
      var i = 0
      while (i < weights.length) { s += weights(i) * f(i); i += 1 }
      s
    }
    
    /** Sum over the data of the gradient w.r.t. weights and intercept. */
    private[ml] def computeGradient(
      data: RDD[ClassificationData],
      weights: Array[Double],
      intercept: Double
    ): (Array[Double], Double) = {
      data.treeAggregate((Array.fill(weights.length)(0.0), 0.0))(
        seqOp = { case ((grad, errSum), point) =>
          val error = sigmoid(margin(weights, point.features, intercept)) - point.label
          val f = point.features.toArray
          var i = 0
          while (i < grad.length) { grad(i) += f(i) * error; i += 1 }
          (grad, errSum + error)
        },
        combOp = { case ((g1, e1), (g2, e2)) =>
          var i = 0
          while (i < g1.length) { g1(i) += g2(i); i += 1 }
          (g1, e1 + e2)
        }
      )
    }
  }
  
  case class SoteriaLogisticRegressionModel(weights: Vector, intercept: Double) {
    def probability(features: Vector): Double =
      SoteriaLogisticRegression.sigmoid(SoteriaLogisticRegression.margin(weights.toArray, features, intercept))
    
    def predict(features: Vector): Double = if (probability(features) >= 0.5) 1.0 else 0.0
  }
  
  /**
   * SOTERIA Collaborative Filtering (ALS, explicit feedback)
   */
  class SoteriaALS(session: SoteriaSession, rank: Int = 10, maxIterations: Int = 20, regParam: Double = 0.01) {
    
    def train(dataset: EncryptedDataset[RecommendationData]): SoteriaALSModel = {
      val (userFactors, itemFactors) = session.executeWithPartitioning(
        dataset,
        "model_update",
        (data: Dataset[RecommendationData]) => {
          val ratingsRDD = data.rdd.cache()
          val rnd = new Random(42L)
          
          val users = ratingsRDD.map(_.user).distinct().collect()
          val items = ratingsRDD.map(_.item).distinct().collect()
          
          var userFactors = users.map(u => (u, Array.fill(rank)(rnd.nextGaussian() * 0.1))).toMap
          var itemFactors = items.map(i => (i, Array.fill(rank)(rnd.nextGaussian() * 0.1))).toMap
          
          for (_ <- 1 to maxIterations) {
            userFactors = SoteriaALS.solveFactors(ratingsRDD.map(r => (r.user, r.item, r.rating)), itemFactors, rank, regParam)
            itemFactors = SoteriaALS.solveFactors(ratingsRDD.map(r => (r.item, r.user, r.rating)), userFactors, rank, regParam)
          }
          
          ratingsRDD.unpersist()
          (userFactors, itemFactors)
        }
      )
      
      SoteriaALSModel(userFactors, itemFactors, rank)
    }
  }
  
  object SoteriaALS {
    /**
     * For each `id`, solve (sum_j f_j f_j^T + lambda * n * I) x = sum_j r_j f_j
     * over its ratings `(id, otherId, rating)`, holding `otherFactors` fixed.
     */
    private[ml] def solveFactors(
      ratings: RDD[(Int, Int, Float)],
      otherFactors: Map[Int, Array[Double]],
      rank: Int,
      regParam: Double
    ): Map[Int, Array[Double]] = {
      val bFactors = ratings.sparkContext.broadcast(otherFactors)
      val result = ratings.map { case (id, other, rating) => (id, (other, rating)) }
        .groupByKey()
        .map { case (id, rs) =>
          val A = Array.ofDim[Double](rank, rank)
          val b = Array.fill(rank)(0.0)
          var n = 0
          rs.foreach { case (other, rating) =>
            val f = bFactors.value(other)
            for (i <- 0 until rank; j <- 0 until rank) A(i)(j) += f(i) * f(j)
            for (i <- 0 until rank) b(i) += rating * f(i)
            n += 1
          }
          for (i <- 0 until rank) A(i)(i) += regParam * n
          (id, solveLinearSystem(A, b))
        }
        .collect()
        .toMap
      bFactors.destroy()
      result
    }
    
    /** Gaussian elimination with partial pivoting. Mutates `A` and `b`. */
    private[soteria] def solveLinearSystem(A: Array[Array[Double]], b: Array[Double]): Array[Double] = {
      val n = b.length
      for (col <- 0 until n) {
        val pivotRow = (col until n).maxBy(r => math.abs(A(r)(col)))
        if (pivotRow != col) {
          val tmpRow = A(col); A(col) = A(pivotRow); A(pivotRow) = tmpRow
          val tmpB = b(col); b(col) = b(pivotRow); b(pivotRow) = tmpB
        }
        val pivot = A(col)(col)
        require(math.abs(pivot) > 1e-12, "singular system")
        for (r <- col + 1 until n) {
          val factor = A(r)(col) / pivot
          for (c <- col until n) A(r)(c) -= factor * A(col)(c)
          b(r) -= factor * b(col)
        }
      }
      val x = Array.fill(n)(0.0)
      for (i <- (n - 1) to 0 by -1) {
        var s = b(i)
        for (j <- i + 1 until n) s -= A(i)(j) * x(j)
        x(i) = s / A(i)(i)
      }
      x
    }
    
    def computeRMSE(ratings: RDD[RecommendationData], model: SoteriaALSModel): Double = {
      math.sqrt(ratings.map(r => math.pow(r.rating - model.predict(r.user, r.item), 2)).mean())
    }
  }
  
  case class SoteriaALSModel(
    userFactors: Map[Int, Array[Double]], 
    itemFactors: Map[Int, Array[Double]], 
    rank: Int
  ) {
    def predict(user: Int, item: Int): Double = {
      (userFactors.get(user), itemFactors.get(item)) match {
        case (Some(userVec), Some(itemVec)) =>
          userVec.zip(itemVec).map { case (u, i) => u * i }.sum
        case _ => 0.0 // Default prediction for cold start
      }
    }
  }
  
  /**
   * Security and Privacy Utilities
   */
  object SecurityUtils {
    
    /**
     * Per-partition Fisher-Yates shuffle (records never leave their partition).
     */
    def secureDataShuffle[T: ClassTag](data: RDD[T], seed: Long = System.currentTimeMillis()): RDD[T] = {
      data.mapPartitionsWithIndex { (index, iterator) =>
        val random = new Random(seed + index)
        val shuffled = iterator.toArray
        
        for (i <- shuffled.length - 1 to 1 by -1) {
          val j = random.nextInt(i + 1)
          val temp = shuffled(i)
          shuffled(i) = shuffled(j)
          shuffled(j) = temp
        }
        
        shuffled.iterator
      }
    }
    
    /**
     * Secure aggregation with integrity checking
     */
    def secureAggregate[T](data: RDD[T], aggregateFunc: (T, T) => T): T = {
      // Basic secure aggregation - integrity checking to be added in future versions
      data.reduce(aggregateFunc)
    }
    
    /* 
     * FUTURE WORK: Advanced security features for SOTERIA v2+
     * The following features are planned for future versions:
     */
    
    /**
     * Attack detection mechanisms (FUTURE WORK)
     * This feature is not implemented in SOTERIA v1.0
     */
    /*
    def detectAnomalousAccess(accessPattern: Seq[String]): Boolean = {
      // TODO: Implement pattern analysis for detecting potential attacks
      // This will include detection of:
      // - Model inversion attacks
      // - Membership inference attacks  
      // - Model extraction attacks
      val suspiciousPatterns = Set("repeated_model_query", "systematic_inference", "gradient_extraction")
      accessPattern.exists(pattern => suspiciousPatterns.contains(pattern))
    }
    */
    
    /**
     * Placeholder for attack detection (always returns false in v1.0)
     */
    def detectAnomalousAccess(accessPattern: Seq[String]): Boolean = {
      // SOTERIA v1.0: Basic implementation - always returns false
      // Advanced attack detection will be implemented in future versions
      false
    }
  }
}
