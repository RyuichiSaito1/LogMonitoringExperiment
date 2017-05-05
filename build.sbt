name := "LogMonitoringExperiment"

version := "1.0"

// For the Scala API, Spark 2.1.0 uses Scala 2.11.X
scalaVersion := "2.11.11"

organization := "jp.ac.keio.sdm"

// Cached resolution is an experimental feature of sbt added since 0.13.7 to address the scalability performance of dependency resolution.
updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies ++= Seq(
  // Must set "provided" to compile normally.
  //"org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0",
  // Scala Logging dependencies
  "org.slf4j" % "slf4j-api" % "1.7.25" % "compile",
  "ch.qos.logback" % "logback-classic" % "1.2.2" % "runtime",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0" % "compile"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}