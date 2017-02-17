package example

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite, Outcome}

trait TestSparkSession
  extends FunSuite
  with BeforeAndAfterEach {

  val log = Logger.getLogger(this.getClass)
  var _spark: SparkSession = null

  protected override def beforeEach(): Unit = {
    if (_spark == null) {
      val master = "local[2]"
      _spark = SparkSession.builder().master(master)getOrCreate()
    }
    super.beforeEach()
  }

  protected override def afterEach(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterEach()
    }
  }

}
