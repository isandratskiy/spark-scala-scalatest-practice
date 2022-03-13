
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.0.1"
ThisBuild / organization := "io.sandratskyi"

lazy val `spark-scala-test-example` =
  project
    .in(file("."))
    .settings(
      name := "spark-scala-scalatest-practice",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core",
        "org.apache.spark" %% "spark-sql"
      ).map(_ % "3.2.0"),
      libraryDependencies ++= Seq(
        "org.scalacheck" %% "scalacheck" % "1.15.1",
        "org.scalatest" %% "scalatest" % "3.2.10"
      )
    )
