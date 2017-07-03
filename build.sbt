lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.11",
      version := "1.0.0"
    )),
    name := "Spark ETL Demo",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "org.apache.spark" %% "spark-core" % "2.1.1",
      "org.apache.spark" %% "spark-sql" % "2.1.1",
      "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
    )
  )
