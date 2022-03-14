package io.sandratskyi.spark

import org.apache.spark.sql.{Dataset, SparkSession}

class SparkCsvRead(sparkSession: SparkSession) {
  def getEmployees: Dataset[Employee] = {
    import sparkSession.implicits._
    sparkSession.read
      .option("header", value = true)
      .csv("src/test/resources/shop_employees.csv").as[Employee]
  }
}

object SparkCsvRead {
  def apply(sparkSession: SparkSession): SparkCsvRead =
    new SparkCsvRead(sparkSession)
}
