package io.sandratskyi.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

class SparkJsonReadTest extends AnyFreeSpec with Matchers {
  private val sparkJsonRead = SparkJsonRead {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark__json__test__")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  "should read from JSON source" in {
    sparkJsonRead.getEmployees.columns.length mustBe 7
  }
}
