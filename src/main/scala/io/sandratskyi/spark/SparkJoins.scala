package io.sandratskyi.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

sealed trait Join {
  def join(left: DataFrame, right: DataFrame): DataFrame = this match {
    case RightOuter =>
      left.join(right, left.col("team") === right.col("team_id"), "right_outer")
    case LeftOuter =>
      left.join(right, left.col("team") === right.col("team_id"), "left_outer")
    case InnerOuter =>
      left.join(right, left.col("team") === right.col("team_id"), "inner")
  }
}

class SparkJoin(sparkSession: SparkSession) {
  private lazy val context: SparkContext = sparkSession.sparkContext

  private lazy val kidsRDD = context.parallelize(List(
    Row(40, "Mary", 1),
    Row(41, "Jane", 3),
    Row(42, "David", 3),
    Row(43, "Angela", 2),
    Row(44, "Charlie", 1),
    Row(45, "Jimmy", 2),
    Row(46, "Lonely", 7)
  ))

  private lazy val teamsRDD = context.parallelize(List(
    Row(1, "The Invincibles"),
    Row(2, "Dog Lovers"),
    Row(3, "Rockstars"),
    Row(4, "The Non-Existent Team")
  ))

  private lazy val kidsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("team", IntegerType),
  ))

  private lazy val teamsSchema = StructType(Array(
    StructField("team_id", IntegerType),
    StructField("team_name", StringType)
  ))

  def createKidsDataFrame: DataFrame =
    sparkSession.createDataFrame(kidsRDD, kidsSchema)

  def createTeamsDataFrame: DataFrame =
    sparkSession.createDataFrame(teamsRDD, teamsSchema)
}

object SparkJoins {
  def apply(sparkSession: SparkSession): SparkJoin = new SparkJoin(sparkSession)
}

case object RightOuter extends Join

case object LeftOuter extends Join

case object InnerOuter extends Join
