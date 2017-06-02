package jp.ac.keio.sdm.LogAggregateExperiment

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by Ryuichi on 6/2/2017 AD.
  */
object LogAggregateExperiment {

  val spark = SparkSession
      .builder()
      .appName("Log Aggregate Experiment")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

  import spark.implicits._
}
