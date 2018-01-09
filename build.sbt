name := "LogMonitoringExperiment"

version := "1.0"

// For the Scala API, Spark 2.1.0 uses Scala 2.11.X
scalaVersion := "2.11.11"

organization := "jp.ac.keio.sdm"

// Cached resolution is an experimental feature of sbt added since 0.13.7 to address the scalability performance of dependency resolution.
updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies ++= Seq(
  // Must set "provided" to compile normally.
  "org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided",
  // "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1",
  // Scala Logging dependencies
  "org.slf4j" % "slf4j-api" % "1.7.25" % "compile",
  "ch.qos.logback" % "logback-classic" % "1.2.2" % "runtime",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0" % "compile",
  // scala-time
  "org.scala-tools.time" % "time_2.9.1" % "0.5",
  // scala-io
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3-1",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3-1",
  // AWS API
  "com.amazonaws" % "aws-java-sdk" % "1.11.259"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}