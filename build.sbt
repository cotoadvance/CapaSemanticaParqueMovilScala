ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"

lazy val root = (project in file("."))
  .settings(
    name := "CapaSemanticaParqueMovilScala"
  )