package examples

import soteria.core.SoteriaCore
import soteria.core.SoteriaCore._
import soteria.ml.SoteriaML._
import soteria.ml.ExtendedSoteriaML._
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.sql.SparkSession
import scala.util.Random

/**
 * Example applications demonstrating SOTERIA usage
 * Based on the IEEE paper "Privacy-Preserving Machine Learning on Apache Spark"
 */
object SoteriaExamples {
  
  def main(args: Array[String]): Unit = {
    println("=== SOTERIA Privacy-Preserving ML Examples ===")
    
    // Configuration
    val config = SoteriaConfig(
      enclaveEnabled = true,
      encryptionEnabled = true,
      partitioningStrategy = "COMPUTATION_PARTITIONING"
    )
    
    // Create SOTERIA session
    val session = SoteriaCore.createSession("SoteriaExamples", config)
    
    try {
      // Run examples for all algorithms from the paper
      println("\n=== Running SOTERIA examples for all paper algorithms ===")
      println("Algorithms: LDA, Naive Bayes, ALS, Logistic Regression, PCA, GBT, Linear Regression, K-Means")
      
      clusteringExample(session)           // K-Means
      classificationExample(session)       // Logistic Regression 
      naiveBayesExample(session)          // Naive Bayes
      recommendationExample(session)       // ALS
      linearRegressionExample(session)     // Linear Regression
      gbtExample(session)                 // Gradient Boosted Trees
      pcaExample(session)                 // PCA
      ldaExample(session)                 // LDA
      securityDemo(session)
      
    } finally {
      session.close()
    }
  }
  
  /**
   * Example: Privacy-preserving K-Means clustering
   */
  def clusteringExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA K-Means Clustering Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic clustering data
    val clusteringData = (1 to 1000).map { i =>
      val cluster = i % 3
      val noise = Array(Random.nextGaussian() * 0.1, Random.nextGaussian() * 0.1)
      val features = cluster match {
        case 0 => Vectors.dense(2.0 + noise(0), 2.0 + noise(1))
        case 1 => Vectors.dense(-2.0 + noise(0), 2.0 + noise(1))
        case 2 => Vectors.dense(0.0 + noise(0), -2.0 + noise(1))
      }
      ClusteringData(features, i.toLong)
    }
    
    val clusteringDF = clusteringData.toDF()
    val encryptedDataset = EncryptedDataset(clusteringDF.as[ClusteringData], session.masterKey)
    
    // Train SOTERIA K-Means model
    val kMeans = new SoteriaKMeans(session, k = 3, maxIterations = 20)
    val model = kMeans.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained K-Means model with ${model.k} clusters")
    println(s"[SOTERIA] Cluster centroids:")
    model.centroids.zipWithIndex.foreach { case (centroid, i) =>
      println(s"  Cluster $i: ${centroid.toArray.mkString(", ")}")
    }
    
    // Test prediction
    val testPoint = Vectors.dense(1.5, 1.5)
    val prediction = model.predict(testPoint)
    println(s"[SOTERIA] Test point (1.5, 1.5) assigned to cluster: $prediction")
  }
  
  /**
   * Example: Privacy-preserving Logistic Regression
   */
  def classificationExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Logistic Regression Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic classification data
    val classificationData = (1 to 1000).map { _ =>
      val x1 = Random.nextGaussian()
      val x2 = Random.nextGaussian()
      val label = if (x1 + x2 + Random.nextGaussian() * 0.1 > 0) 1.0 else 0.0
      ClassificationData(Vectors.dense(x1, x2), label)
    }
    
    val classificationDF = classificationData.toDF()
    val encryptedDataset = EncryptedDataset(classificationDF.as[ClassificationData], session.masterKey)
    
    // Train SOTERIA Logistic Regression model
    val logisticRegression = new SoteriaLogisticRegression(session, maxIterations = 100, stepSize = 1.0)
    val model = logisticRegression.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained Logistic Regression model")
    println(s"[SOTERIA] Weights: ${model.weights.toArray.mkString(", ")}")
    println(s"[SOTERIA] Intercept: ${model.intercept}")
    
    // Test prediction
    val testFeatures = Vectors.dense(1.0, 1.0)
    val prediction = model.predict(testFeatures)
    println(s"[SOTERIA] Test features (1.0, 1.0) prediction: $prediction")
  }
  
  /**
   * Example: Privacy-preserving Collaborative Filtering (ALS)
   */
  def recommendationExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Collaborative Filtering (ALS) Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic recommendation data
    val users = 1 to 100
    val items = 1 to 50
    val recommendationData = for {
      user <- users
      item <- items if Random.nextDouble() > 0.7 // Sparse ratings
    } yield {
      // Simulate user preferences
      val userBias = Random.nextGaussian() * 0.5
      val itemBias = Random.nextGaussian() * 0.5
      val interaction = Random.nextGaussian() * 0.3
      val rating = math.max(1.0, math.min(5.0, 3.0 + userBias + itemBias + interaction))
      RecommendationData(user, item, rating.toFloat)
    }
    
    val recommendationDF = recommendationData.toDF()
    val encryptedDataset = EncryptedDataset(recommendationDF.as[RecommendationData], session.masterKey)
    
    // Train SOTERIA ALS model
    val als = new SoteriaALS(session, rank = 5, maxIterations = 10, regParam = 0.1)
    val model = als.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained ALS model with rank ${model.rank}")
    println(s"[SOTERIA] Number of user factors: ${model.userFactors.size}")
    println(s"[SOTERIA] Number of item factors: ${model.itemFactors.size}")
    
    // Test prediction
    val testUser = 1
    val testItem = 1
    val prediction = model.predict(testUser, testItem)
    println(s"[SOTERIA] Predicted rating for user $testUser, item $testItem: $prediction")
  }
  
  /**
   * Example: Privacy-preserving Naive Bayes
   */
  def naiveBayesExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Naive Bayes Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic classification data for Naive Bayes
    val classificationData = (1 to 1000).map { _ =>
      val x1 = if (Random.nextBoolean()) 1.0 else 0.0
      val x2 = if (Random.nextBoolean()) 1.0 else 0.0
      val x3 = if (Random.nextBoolean()) 1.0 else 0.0
      val label = if ((x1 + x2 + x3) >= 2) 1.0 else 0.0
      ClassificationData(Vectors.dense(x1, x2, x3), label)
    }
    
    val classificationDF = classificationData.toDF()
    val encryptedDataset = EncryptedDataset(classificationDF.as[ClassificationData], session.masterKey)
    
    // Train SOTERIA Naive Bayes model
    val naiveBayes = new SoteriaNaiveBayes(session)
    val model = naiveBayes.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained Naive Bayes model")
    println(s"[SOTERIA] Model type: ${model.getClass.getSimpleName}")
  }
  
  /**
   * Example: Privacy-preserving Linear Regression
   */
  def linearRegressionExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Linear Regression Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic regression data
    val regressionData = (1 to 1000).map { _ =>
      val x1 = Random.nextGaussian()
      val x2 = Random.nextGaussian()
      val label = 2.0 * x1 + 3.0 * x2 + Random.nextGaussian() * 0.1
      RegressionData(Vectors.dense(x1, x2), label)
    }
    
    val regressionDF = regressionData.toDF()
    val encryptedDataset = EncryptedDataset(regressionDF.as[RegressionData], session.masterKey)
    
    // Train SOTERIA Linear Regression model
    val linearRegression = new SoteriaLinearRegression(session, maxIter = 100, regParam = 0.1)
    val model = linearRegression.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained Linear Regression model")
    println(s"[SOTERIA] Coefficients: ${model.coefficients.toArray.mkString(", ")}")
    println(s"[SOTERIA] Intercept: ${model.intercept}")
  }
  
  /**
   * Example: Privacy-preserving Gradient Boosted Trees
   */
  def gbtExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Gradient Boosted Trees Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic regression data
    val regressionData = (1 to 1000).map { _ =>
      val x1 = Random.nextGaussian()
      val x2 = Random.nextGaussian()
      val label = x1 * x1 + x2 * x2 + Random.nextGaussian() * 0.1
      RegressionData(Vectors.dense(x1, x2), label)
    }
    
    val regressionDF = regressionData.toDF()
    val encryptedDataset = EncryptedDataset(regressionDF.as[RegressionData], session.masterKey)
    
    // Train SOTERIA GBT model
    val gbt = new SoteriaGBT(session, maxIter = 50)
    val model = gbt.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained Gradient Boosted Trees model")
    println(s"[SOTERIA] Number of trees: ${model.getNumTrees}")
    println(s"[SOTERIA] Total number of nodes: ${model.totalNumNodes}")
  }
  
  /**
   * Example: Privacy-preserving PCA
   */
  def pcaExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA PCA Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic high-dimensional data
    val pcaData = (1 to 1000).map { i =>
      val features = Vectors.dense(Array.fill(10)(Random.nextGaussian()))
      PCAData(features, i.toLong)
    }
    
    val pcaDF = pcaData.toDF()
    val encryptedDataset = EncryptedDataset(pcaDF.as[PCAData], session.masterKey)
    
    // Train SOTERIA PCA model
    val pca = new SoteriaPCA(session, k = 3)
    val model = pca.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained PCA model")
    println(s"[SOTERIA] Reduced dimensionality from 10 to 3")
  }
  
  /**
   * Example: Privacy-preserving LDA (Topic Modeling)
   */
  def ldaExample(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA LDA (Topic Modeling) Example ---")
    
    import session.spark.implicits._
    
    // Generate synthetic document-term data
    val ldaData = (1 to 100).map { docId =>
      // Simulate document as term frequency vector
      val features = Vectors.dense(Array.fill(50)(Random.nextInt(10).toDouble))
      LDAData(features, docId.toLong)
    }
    
    val ldaDF = ldaData.toDF()
    val encryptedDataset = EncryptedDataset(ldaDF.as[LDAData], session.masterKey)
    
    // Train SOTERIA LDA model
    val lda = new SoteriaLDA(session, k = 5, maxIter = 20)
    val model = lda.train(encryptedDataset)
    
    println(s"[SOTERIA] Trained LDA model")
    println(s"[SOTERIA] Number of topics: ${model.getK}")
    println(s"[SOTERIA] Vocabulary size: ${model.vocabSize}")
  }
  
  /**
   * Shows how operations are classified into computation zones.
   */
  def securityDemo(session: SoteriaSession): Unit = {
    println("\n--- SOTERIA Computation Partitioning ---")
    
    val operations = List(
      "gradient_computation", "model_update", "feature_extraction",
      "data_loading", "result_aggregation", "visualization"
    )
    
    operations.foreach { op =>
      val location = ComputationPartitioner.getComputationZone(op) match {
        case ComputationPartitioner.EnclaveZone => "enclave"
        case ComputationPartitioner.UntrustedZone => "untrusted"
      }
      println(s"  $op -> $location")
    }
    
    println("\n[SOTERIA] Rebuild status: zones are classified but not yet enforced,")
    println("  and datasets are read as plaintext. See the v2.0 plan in the README.")
  }
}
