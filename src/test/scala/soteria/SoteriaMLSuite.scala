package soteria

import scala.util.Random

import org.apache.spark.ml.linalg.Vectors
import soteria.core.SoteriaCore._
import soteria.ml.ExtendedSoteriaML._
import soteria.ml.SoteriaML._

class SoteriaMLSuite extends SparkTestBase {

  private def wrap[T](data: org.apache.spark.sql.Dataset[T]) = EncryptedDataset(data, session.masterKey)

  test("K-Means recovers well-separated clusters") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(7)
    val centers = Array(Vectors.dense(5.0, 5.0), Vectors.dense(-5.0, 5.0), Vectors.dense(0.0, -5.0))
    val data = (0 until 300).map { i =>
      val c = centers(i % 3)
      ClusteringData(Vectors.dense(c(0) + rnd.nextGaussian() * 0.2, c(1) + rnd.nextGaussian() * 0.2), i.toLong)
    }
    val model = new SoteriaKMeans(session, k = 3, maxIterations = 20, seed = 1L).train(wrap(data.toDS()))

    assert(model.centroids.length == 3)
    centers.foreach { c =>
      val nearest = model.centroids.map(m => Vectors.sqdist(m, c)).min
      assert(nearest < 0.1, s"no centroid near $c: ${model.centroids.mkString(", ")}")
    }
    assert(model.predict(Vectors.dense(4.8, 5.1)) != model.predict(Vectors.dense(-4.9, 5.0)))
  }

  test("logistic regression separates linearly separable data") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(11)
    val data = (0 until 500).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      ClassificationData(Vectors.dense(x1, x2), if (x1 + x2 > 0) 1.0 else 0.0)
    }
    val model = new SoteriaLogisticRegression(session, maxIterations = 100, stepSize = 1.0).train(wrap(data.toDS()))

    val accuracy = data.count(p => model.predict(p.features) == p.label).toDouble / data.size
    assert(accuracy > 0.95, s"accuracy $accuracy")
    assert(model.weights(0) > 0 && model.weights(1) > 0)
  }

  test("ALS fits low-rank ratings better than the global mean") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(3)
    val u = Array.fill(40)(Array.fill(2)(rnd.nextGaussian()))
    val v = Array.fill(30)(Array.fill(2)(rnd.nextGaussian()))
    val data = for {
      i <- 0 until 40; j <- 0 until 30 if rnd.nextDouble() < 0.6
    } yield RecommendationData(i, j, (3.0 + u(i)(0) * v(j)(0) + u(i)(1) * v(j)(1)).toFloat)

    val model = new SoteriaALS(session, rank = 3, maxIterations = 10, regParam = 0.01).train(wrap(data.toDS()))
    val ratings = spark.sparkContext.parallelize(data)
    val rmse = SoteriaALS.computeRMSE(ratings, model)
    val mean = data.map(_.rating.toDouble).sum / data.size
    val baseline = math.sqrt(data.map(r => math.pow(r.rating - mean, 2)).sum / data.size)

    assert(model.userFactors.size == 40 && model.itemFactors.size == 30)
    assert(rmse < 0.5 * baseline, s"rmse $rmse vs baseline $baseline")
  }

  test("linear system solver handles a zero leading pivot") {
    val x = SoteriaALS.solveLinearSystem(Array(Array(0.0, 1.0), Array(2.0, 0.0)), Array(3.0, 4.0))
    assert(math.abs(x(0) - 2.0) < 1e-9 && math.abs(x(1) - 3.0) < 1e-9)
  }

  test("linear regression recovers coefficients") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(5)
    val data = (0 until 500).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      RegressionData(Vectors.dense(x1, x2), 2.0 * x1 + 3.0 * x2 + rnd.nextGaussian() * 0.01)
    }
    val model = new SoteriaLinearRegression(session, maxIter = 50, regParam = 0.0).train(wrap(data.toDS()))
    assert(math.abs(model.coefficients(0) - 2.0) < 0.05)
    assert(math.abs(model.coefficients(1) - 3.0) < 0.05)
  }

  test("naive Bayes, GBT, PCA and LDA train") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(9)

    val nbData = (0 until 200).map { _ =>
      val f = Array.fill(3)(if (rnd.nextBoolean()) 1.0 else 0.0)
      ClassificationData(Vectors.dense(f), if (f.sum >= 2) 1.0 else 0.0)
    }
    assert(new SoteriaNaiveBayes(session).train(wrap(nbData.toDS())).numClasses == 2)

    val gbtData = (0 until 200).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      RegressionData(Vectors.dense(x1, x2), x1 * x1 + x2 * x2)
    }
    assert(new SoteriaGBT(session, maxIter = 5).train(wrap(gbtData.toDS())).getNumTrees == 5)

    val pcaData = (0 until 200).map(i => PCAData(Vectors.dense(Array.fill(10)(rnd.nextGaussian())), i.toLong))
    val pca = new SoteriaPCA(session, k = 3).train(wrap(pcaData.toDS()))
    assert(pca.pc.numRows == 10 && pca.pc.numCols == 3)

    val ldaData = (0 until 50).map(i => LDAData(Vectors.dense(Array.fill(20)(rnd.nextInt(5).toDouble)), i.toLong))
    val lda = new SoteriaLDA(session, k = 4, maxIter = 5).train(wrap(ldaData.toDS()))
    assert(lda.getK == 4 && lda.vocabSize == 20)
  }
}
