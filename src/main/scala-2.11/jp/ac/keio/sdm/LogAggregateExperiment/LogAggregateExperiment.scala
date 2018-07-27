/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */
package jp.ac.keio.sdm.LogAggregateExperiment

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by Ryuichi on 6/2/2017 AD.
  */
object LogAggregateExperiment {

  val ThreadCount = "*"
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "LogAggregateExperiment"
  val BatchDuration = new Duration(120 * 1000)
  val UpperLimit = 10000
  val DateTime = "date_time"
  val LogLevel = "log_level"
  val MultiThreadId = "multi_thread_id"
  val Message = "message"
  val StackTrace01 = "stack_trace_01"
  val StackTrace02 = "stack_trace_02"
  val StackTrace03 = "stack_trace_03"
  val StackTrace04 = "stack_trace_04"
  val StackTrace05 = "stack_trace_05"
  val StackTrace06 = "stack_trace_06"
  val StackTrace07 = "stack_trace_07"
  val StackTrace08 = "stack_trace_08"
  val StackTrace09 = "stack_trace_09"
  val StackTrace10 = "stack_trace_10"
  val StackTrace11 = "stack_trace_11"
  val StackTrace12 = "stack_trace_12"
  val StackTrace13 = "stack_trace_13"
  val Number = "Number"
  val PartitionNum = 1

  // Development Mode.
  val SavingDirectoryForErrorLog = "data/csv"
  // Product Mode.
  // val SavingDirectoryForErrorLog = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data/csv"
  // val SavingDirectoryForErrorLog = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data_20180727/csv"

  // Development Mode.
  val SavingDirectoryForAggregateData = "data/parquet"
  // Product Mode.
  // val SavingDirectoryForAggregateData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data/parquet"
  // val SavingDirectoryForAggregateData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data_20180727/parquet"

  def main(args: Array[String]) {

    if (args.length < 2) {
      // stripMargin : Strip a leading prefix consisting of blanks or control characters followed by | from the line.
      System.err.println(s"""
                       |Usage: DirectKafkaWordCount <brokers> <topics>
                       |  <brokers> is a list of one or more Kafka brokers
                       |  <topics> is a list of one or more kafka topics to consume from
                       |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args
    // Development Mode.
    val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    // Product Mode.
    // val sparkConf = new SparkConf().setAppName(ApplicationName)
    val ssc = new StreamingContext(sparkConf, BatchDuration)
    val spark = SparkSession
      .builder()
      .appName(ApplicationName)
      // .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    // "map(_._2)" equals "map(x => (x._2))"
    // x._2 returns the second element of a tuple
    val lines = messages.map(_._2)

       // https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
       // Count according to message type.
       lines.foreachRDD(jsonRDD => {
       val data = spark.read.option("wholeFile", true).json(jsonRDD)
       if (data.count() > 0) {
         import org.apache.spark.sql.functions._
         val resultSetByGroupBy = data.groupBy(
           LogLevel
           ,MultiThreadId
           //,Message
           ,StackTrace01
           ,StackTrace02
           ,StackTrace03
           ,StackTrace04
           ,StackTrace05
           ,StackTrace06
           ,StackTrace07
           ,StackTrace08
           ,StackTrace09
           ,StackTrace10
           ,StackTrace11
           ,StackTrace12
           ,StackTrace13)
           .agg(
             count(StackTrace01).alias(Number),
             first(Message).alias(Message))

         // Append Exact Date and Time.
         val dateTime = new DateTime()
         val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
         val formattedDateTime = formatter.print(dateTime)
         val printingResultSetByGroupBy = resultSetByGroupBy.withColumn(DateTime, lit(formattedDateTime))
            .select(DateTime
              ,Message
              ,LogLevel
              ,MultiThreadId
              ,StackTrace01
              ,StackTrace02
              ,StackTrace03
              ,StackTrace04
              ,StackTrace05
              ,StackTrace06
              ,StackTrace07
              ,StackTrace08
              ,StackTrace09
              ,StackTrace10
              ,StackTrace11
              ,StackTrace12
              ,StackTrace13
              ,Number)

         data.show(UpperLimit)

         printingResultSetByGroupBy.show(UpperLimit)
         printingResultSetByGroupBy.coalesce(PartitionNum).write.mode(SaveMode.Append).csv(SavingDirectoryForErrorLog)
         // data.coalesce(PartitionNum).write.mode(SaveMode.Append).parquet(SavingDirectoryForAggregateData)
         printingResultSetByGroupBy.coalesce(PartitionNum).write.mode(SaveMode.Append).parquet(SavingDirectoryForAggregateData)
       }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}