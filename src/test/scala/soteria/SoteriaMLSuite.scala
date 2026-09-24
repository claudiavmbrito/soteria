package soteria

import scala.util.Random

import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import soteria.core.SoteriaCore._
import soteria.ml.ExtendedSoteriaML._
import soteria.ml.SoteriaML._

/** Accuracy parity of the SOTERIA trainers with vanilla MLlib. */
class SoteriaMLSuite extends SparkTestBase {

  private def wrap[T](data: org.apache.spark.sql.Dataset[T]) = EncryptedDataset(data, session.masterKey)

  private def assertClose(a: Double, b: Double, tol: Double, what: String): Unit =
    assert(math.abs(a - b) <= tol, s"$what: $a vs $b (tol $tol)")

  test("K-Means recovers clusters and matches MLlib's cost") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(7)
    val centers = Array(Vectors.dense(5.0, 5.0), Vectors.dense(-5.0, 5.0), Vectors.dense(0.0, -5.0))
    val data = (0 until 300).map { i =>
      val c = centers(i % 3)
      ClusteringData(Vectors.dense(c(0) + rnd.nextGaussian() * 0.5, c(1) + rnd.nextGaussian() * 0.5), i.toLong)
    }
    val model = new SoteriaKMeans(session, k = 3, maxIterations = 50, seed = 1L).train(wrap(data.toDS()))

    centers.foreach { c =>
      assert(model.centroids.map(m => Vectors.sqdist(m, c)).min < 0.1, s"no centroid near $c")
    }
    val mllib = new KMeans().setK(3).setSeed(1L).fit(data.toDF())
    val points = data.map(_.features)
    val mllibCost = points.map(p => Vectors.sqdist(p, mllib.clusterCenters(mllib.predict(p)))).sum
    assertClose(model.cost(points), mllibCost, 0.01 * mllibCost, "k-means cost")
  }

  test("logistic regression matches MLlib (L2, no standardization)") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(11)
    val data = (0 until 1000).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      val p = 1 / (1 + math.exp(-(1.5 * x1 - 2.0 * x2 + 0.5)))
      ClassificationData(Vectors.dense(x1, x2), if (rnd.nextDouble() < p) 1.0 else 0.0)
    }
    val model = new SoteriaLogisticRegression(session, maxIterations = 200, regParam = 0.1, tol = 1e-10)
      .train(wrap(data.toDS()))
    val mllib = new LogisticRegression().setRegParam(0.1).setStandardization(false)
      .setMaxIter(200).setTol(1e-10).fit(data.toDF())

    for (j <- 0 until 2) assertClose(model.weights(j), mllib.coefficients(j), 1e-4, s"weight $j")
    assertClose(model.intercept, mllib.intercept, 1e-4, "intercept")
  }

  test("linear regression matches MLlib's normal-equation solver, and ridge shrinks") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(5)
    val data = (0 until 500).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian() * 3 + 1
      RegressionData(Vectors.dense(x1, x2), 2.0 * x1 + 3.0 * x2 - 1.0 + rnd.nextGaussian() * 0.1)
    }
    val ols = new SoteriaLinearRegression(session).train(wrap(data.toDS()))
    val mllib = new LinearRegression().setSolver("normal").setRegParam(0.0).setStandardization(false).fit(data.toDF())
    for (j <- 0 until 2) assertClose(ols.coefficients(j), mllib.coefficients(j), 1e-8, s"coefficient $j")
    assertClose(ols.intercept, mllib.intercept, 1e-8, "intercept")

    val ridge = new SoteriaLinearRegression(session, regParam = 1.0).train(wrap(data.toDS()))
    assert(Vectors.norm(ridge.coefficients, 2) < Vectors.norm(ols.coefficients, 2))
  }

  test("naive Bayes matches MLlib's multinomial model") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(9)
    val data = (0 until 600).map { i =>
      val c = i % 3
      ClassificationData(Vectors.dense(Array.tabulate(4)(j => rnd.nextInt(3 + (if (j == c) 5 else 0)).toDouble)), c.toDouble)
    }
    val model = new SoteriaNaiveBayes(session).train(wrap(data.toDS()))
    val mllib = new NaiveBayes().setModelType("multinomial").setSmoothing(1.0).fit(data.toDF())

    assert(model.numClasses == 3)
    for (c <- 0 until 3) {
      assertClose(model.pi(c), mllib.pi(c), 1e-9, s"pi($c)")
      for (j <- 0 until 4) assertClose(model.theta(c, j), mllib.theta(c, j), 1e-9, s"theta($c,$j)")
    }
    data.take(50).foreach(p => assert(model.predict(p.features) == mllib.predict(p.features)))
  }

  test("PCA matches MLlib up to component sign") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(13)
    val scales = Array(5.0, 3.0, 2.0, 1.0, 0.5)
    val data = (0 until 400).map { i =>
      val z = Array.tabulate(5)(j => rnd.nextGaussian() * scales(j))
      PCAData(Vectors.dense(z(0) + z(1), z(1) - z(2), z(2), z(3) + 0.5 * z(0), z(4)), i.toLong)
    }
    val model = new SoteriaPCA(session, k = 3).train(wrap(data.toDS()))
    val mllib = new PCA().setK(3).setInputCol("features").fit(data.toDF())

    for (c <- 0 until 3) {
      assertClose(model.explainedVariance(c), mllib.explainedVariance(c), 1e-9, s"explained variance $c")
      val sign = math.signum(model.pc(0, c) * mllib.pc(0, c))
      for (r <- 0 until 5) assertClose(model.pc(r, c), sign * mllib.pc(r, c), 1e-6, s"pc($r,$c)")
    }
    val v: Vector = data.head.features
    assert(model.transform(v).size == 3)
  }

  test("enclave-only ALS, GBT and LDA wrap MLlib") {
    val spark = session.spark
    import spark.implicits._
    val rnd = new Random(3)
    val u = Array.fill(40)(Array.fill(2)(rnd.nextGaussian()))
    val v = Array.fill(30)(Array.fill(2)(rnd.nextGaussian()))
    val ratings = for {
      i <- 0 until 40; j <- 0 until 30 if rnd.nextDouble() < 0.6
    } yield RecommendationData(i, j, (3.0 + u(i)(0) * v(j)(0) + u(i)(1) * v(j)(1)).toFloat)
    val als = new SoteriaALS(session, rank = 3, maxIterations = 10, regParam = 0.01).train(wrap(ratings.toDS()))
    val rmse = new RegressionEvaluator().setLabelCol("rating").setMetricName("rmse").evaluate(als.transform(ratings.toDF()))
    val mean = ratings.map(_.rating.toDouble).sum / ratings.size
    val baseline = math.sqrt(ratings.map(r => math.pow(r.rating - mean, 2)).sum / ratings.size)
    assert(rmse < 0.5 * baseline, s"ALS rmse $rmse vs baseline $baseline")

    val gbtData = (0 until 200).map { _ =>
      val x1 = rnd.nextGaussian(); val x2 = rnd.nextGaussian()
      RegressionData(Vectors.dense(x1, x2), x1 * x1 + x2 * x2)
    }
    assert(new SoteriaGBT(session, maxIter = 5).train(wrap(gbtData.toDS())).getNumTrees == 5)

    val ldaData = (0 until 50).map(i => LDAData(Vectors.dense(Array.fill(20)(rnd.nextInt(5).toDouble)), i.toLong))
    val lda = new SoteriaLDA(session, k = 4, maxIter = 5).train(wrap(ldaData.toDS()))
    assert(lda.getK == 4 && lda.vocabSize == 20)
  }
}
