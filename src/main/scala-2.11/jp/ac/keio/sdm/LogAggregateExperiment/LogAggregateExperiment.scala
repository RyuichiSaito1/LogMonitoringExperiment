package jp.ac.keio.sdm.LogAggregateExperiment

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by Ryuichi on 6/2/2017 AD.
  */
object LogAggregateExperiment {

  val ThreadCount = 2
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "LogAggregateExperiment"
  val BatchDuration = 2
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
  val StackTrace14 = "stack_trace_14"
  val PartitionNum = 1
  // val SavingDirectoryForErrorLog = "s3://aws-logs-757020086170-us-west-2/logs/error"
  val SavingDirectoryForErrorLog = "logs/error"
  // val SavingDirectoryForSampleData = "s3://aws-logs-757020086170-us-west-2/logs/error_sample"
  val SavingDirectoryForSampleData = "logs/error_sample"

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
    val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    val ssc = new StreamingContext(sparkConf, Seconds(BatchDuration))
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
     lines.foreachRDD(jsonRDD => {
       val data = spark.read.option("wholeFile", true).json(jsonRDD)
       if (data.count() > 0) {
         import org.apache.spark.sql.functions._
         val resultSetByGroupBy = data.groupBy(
           LogLevel
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
           ,StackTrace14).agg(min(Message).alias(Message))

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
              ,StackTrace14)
         printingResultSetByGroupBy.show(UpperLimit)
         // org.apache.spark.rdd#coalesce : Return a new RDD that is reduced into numPartitions partitions.
         printingResultSetByGroupBy.rdd.coalesce(PartitionNum, true).saveAsTextFile(SavingDirectoryForErrorLog)
         printingResultSetByGroupBy.coalesce(PartitionNum).write.mode("append")parquet(SavingDirectoryForSampleData)
       }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}