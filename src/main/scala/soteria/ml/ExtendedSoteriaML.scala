package soteria.ml

import soteria.core.SoteriaCore._
import soteria.ml.SoteriaML.{ClassificationData, LDAData, PCAData, RegressionData}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, PCA => ML_PCA}
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.ml.clustering.{KMeans, LDA, LDAModel}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, GBTRegressor, GBTRegressionModel}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.util.Random

/**
 * Extended SOTERIA Machine Learning Implementation
 * Includes additional algorithms: LDA, Naive Bayes, PCA, GBT, Linear Regression
 */
object ExtendedSoteriaML {
  
  // Linear Regression
  class SoteriaLinearRegression(session: SoteriaSession, maxIter: Int = 100, regParam: Double = 0.3) {
    def train(dataset: EncryptedDataset[RegressionData]): LinearRegressionModel = {
      // Sensitive zone (enclave, once enforced)
      val result = session.executeWithPartitioning(
        dataset,
        "model_update",
        (data: Dataset[RegressionData]) => {
          
          val lr = new LinearRegression()
            .setMaxIter(maxIter)
            .setRegParam(regParam)
            .setFeaturesCol("features")
            .setLabelCol("label")
          
          val model = lr.fit(data)
          model
        }
      )
      result
    }
  }
  
  // Gradient Boosted Trees
  class SoteriaGBT(session: SoteriaSession, maxIter: Int = 100) {
    def train(dataset: EncryptedDataset[RegressionData]): GBTRegressionModel = {
      // Sensitive zone (enclave, once enforced)
      val result = session.executeWithPartitioning(
        dataset,
        "model_update",
        (data: Dataset[RegressionData]) => {
          
          val gbt = new GBTRegressor()
            .setMaxIter(maxIter)
            .setFeaturesCol("features")
            .setLabelCol("label")
          
          val model = gbt.fit(data)
          model
        }
      )
      result
    }
  }
  
  // PCA
  class SoteriaPCA(session: SoteriaSession, k: Int) {
    def train(dataset: EncryptedDataset[PCAData]): PCAModel = {
      // Sensitive zone (enclave, once enforced)
      val result = session.executeWithPartitioning(
        dataset,
        "feature_extraction",
        (data: Dataset[PCAData]) => {
          
          val assembler = new VectorAssembler()
            .setInputCols(Array("features"))
            .setOutputCol("features_vector")
          
          val dataFrame = assembler.transform(data)

          val pca = new ML_PCA()
            .setK(k)
            .setInputCol("features_vector")
            .setOutputCol("pca_features")

          val model = pca.fit(dataFrame)
          model
        }
      )
      result
    }
  }
  
  // LDA
  class SoteriaLDA(session: SoteriaSession, k: Int, maxIter: Int = 10) {
    def train(dataset: EncryptedDataset[LDAData]): LDAModel = {
      // Sensitive zone (enclave, once enforced)
      val result = session.executeWithPartitioning(
        dataset,
        "model_update",
        (data: Dataset[LDAData]) => {
          
          val lda = new LDA()
            .setK(k)
            .setMaxIter(maxIter)
            .setFeaturesCol("features")
          
          val model = lda.fit(data)
          model
        }
      )
      result
    }
  }
  
  // Naive Bayes
  class SoteriaNaiveBayes(session: SoteriaSession) {
    def train(dataset: EncryptedDataset[ClassificationData]): NaiveBayesModel = {
      // Sensitive zone (enclave, once enforced)
      val result = session.executeWithPartitioning(
        dataset,
        "model_update",
        (data: Dataset[ClassificationData]) => {
          
          val nb = new NaiveBayes()
            .setFeaturesCol("features")
            .setLabelCol("label")
          
          val model = nb.fit(data)
          model
        }
      )
      result
    }
  }
}

