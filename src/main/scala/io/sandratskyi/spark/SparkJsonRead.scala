package io.sandratskyi.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkJsonRead(sparkSession: SparkSession) {
  def getEmployees: DataFrame = sparkSession.read
    .option("multiline", "true")
    .json("src/test/resources/shop_employees.json")
}

object SparkJsonRead {
  def apply(sparkSession: SparkSession): SparkJsonRead = new SparkJsonRead(sparkSession)
}
