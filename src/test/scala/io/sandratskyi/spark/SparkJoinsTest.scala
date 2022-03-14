package io.sandratskyi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkJoinsTest
  extends AnyFunSuite
  with Matchers {

  val sparkJoin: SparkJoin =
    SparkJoins {
      val spark: SparkSession =
        builder()
          .appName("spark__joins__test__")
          .master("local[*]")
          .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")
      spark
    }

  test("should join left outer") {
    val teams = sparkJoin.createTeamsDataFrame
    val kids = sparkJoin.createKidsDataFrame
    val result = LeftOuter.join(kids, teams)

    result.select("team_id")
      .filter(col("id").contains(46))
      .first.getString(0) shouldBe null
  }

  test("should inner join") {
    val teams = sparkJoin.createTeamsDataFrame
    val kids = sparkJoin.createKidsDataFrame
    val result = InnerOuter.join(kids, teams)

    assertThrows[NoSuchElementException] {
      result.select("id")
        .filter(col("team_id").contains(4))
        .first
    }
  }

  test("should join right outer") {
    val teams = sparkJoin.createTeamsDataFrame
    val kids = sparkJoin.createKidsDataFrame
    val result = RightOuter.join(kids, teams)

    result.select("id")
      .filter(col("team_id").contains(4))
      .first.getString(0) shouldBe null
  }
}
