package io.sandratskyi.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

class SparkCsvReadTest extends AnyFreeSpec with Matchers {
  private val sparkCsvRead = SparkCsvRead {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark__csv__test__")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  "should read from CSV source" in {
    sparkCsvRead.getEmployees.columns.length mustBe 7
  }
}
