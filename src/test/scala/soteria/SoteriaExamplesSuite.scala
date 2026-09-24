package soteria

import org.scalatest.funsuite.AnyFunSuite

class SoteriaExamplesSuite extends AnyFunSuite {
  test("example application runs end to end") {
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.ui.enabled", "false")
    examples.SoteriaExamples.main(Array.empty)
  }
}
