package soteria.ml

import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import soteria.core.SoteriaCore._
import soteria.ml.SoteriaML.{LDAData, RecommendationData, RegressionData}

/**
 * Enclave-only trainers: thin wrappers around MLlib.
 *
 * MLlib creates its own stages, which use the application's default resource
 * profile, and that profile is the enclave one. So in both SML-1 and SML-2
 * these algorithms run entirely inside enclaves; nothing is placed on
 * untrusted executors. Explicit SML-2 partitioning for them is future work.
 */
object ExtendedSoteriaML {

  /** Collaborative filtering (explicit feedback). */
  class SoteriaALS(session: SoteriaSession, rank: Int = 10, maxIterations: Int = 10, regParam: Double = 0.1, seed: Long = 42L) {
    def train(dataset: EncryptedDataset[RecommendationData]): ALSModel =
      new ALS()
        .setRank(rank)
        .setMaxIter(maxIterations)
        .setRegParam(regParam)
        .setSeed(seed)
        .setUserCol("user")
        .setItemCol("item")
        .setRatingCol("rating")
        .setColdStartStrategy("drop")
        .fit(dataset.data)
  }

  /** Gradient boosted regression trees. */
  class SoteriaGBT(session: SoteriaSession, maxIter: Int = 20, seed: Long = 42L) {
    def train(dataset: EncryptedDataset[RegressionData]): GBTRegressionModel =
      new GBTRegressor()
        .setMaxIter(maxIter)
        .setSeed(seed)
        .setFeaturesCol("features")
        .setLabelCol("label")
        .fit(dataset.data)
  }

  /** Topic modeling over term-count vectors. */
  class SoteriaLDA(session: SoteriaSession, k: Int, maxIter: Int = 10, seed: Long = 42L) {
    def train(dataset: EncryptedDataset[LDAData]): LDAModel =
      new LDA()
        .setK(k)
        .setMaxIter(maxIter)
        .setSeed(seed)
        .setFeaturesCol("features")
        .fit(dataset.data)
  }
}
