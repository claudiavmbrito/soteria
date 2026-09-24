package soteria

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import soteria.core.SoteriaCore
import soteria.core.SoteriaCore.SoteriaSession

/** Shares one local SOTERIA session per suite. */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {

  @transient protected var session: SoteriaSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("spark.master", "local[2]")
    System.setProperty("spark.ui.enabled", "false")
    System.setProperty("spark.sql.shuffle.partitions", "4")
    session = SoteriaCore.createSession(getClass.getSimpleName)
  }

  override def afterAll(): Unit = {
    try if (session != null) session.close()
    finally super.afterAll()
  }
}
