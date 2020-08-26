name := "StreamTableJoinDemo"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0-beta",
  "joda-time" % "joda-time" % "2.10.2"
)

libraryDependencies ++= sparkDependencies
