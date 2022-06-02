version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.15"

name := "BigDataII"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1"
)
