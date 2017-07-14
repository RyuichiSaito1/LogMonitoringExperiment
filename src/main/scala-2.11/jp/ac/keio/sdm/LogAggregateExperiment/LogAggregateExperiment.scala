package jp.ac.keio.sdm.LogAggregateExperiment

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ryuichi on 6/2/2017 AD.
  */
object LogAggregateExperiment {

  final val threadCount = 2
  final val applicationName = "LogAggregateExperiment"
  final val batchDuration = 2
  final val upperLimit = 10000
  final val log_level = "log_level"
  final val multi_thread_id = "multi_thread_id"
  final val message = "message"
  final val stack_trace_01 = "stack_trace_01"
  final val stack_trace_02 = "stack_trace_02"
  final val stack_trace_03 = "stack_trace_03"
  final val stack_trace_04 = "stack_trace_04"
  final val stack_trace_05 = "stack_trace_05"
  final val stack_trace_06 = "stack_trace_06"
  final val stack_trace_07 = "stack_trace_07"
  final val stack_trace_08 = "stack_trace_08"
  final val stack_trace_09 = "stack_trace_09"
  final val stack_trace_10 = "stack_trace_10"
  final val stack_trace_11 = "stack_trace_11"
  final val stack_trace_12 = "stack_trace_12"
  final val stack_trace_13 = "stack_trace_13"
  final val stack_trace_14 = "stack_trace_14"


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
    val sparkUrl = "local[" + threadCount + "]"
    val sparkConf = new SparkConf().setMaster(sparkUrl).setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration))
    val spark = SparkSession
      .builder()
      .appName(applicationName)
      /*.config("spark.some.config.option", "some-value")*/
      .getOrCreate()

    /*ssc.checkpoint("checkpoint")*/

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // "map(_._2)" equals "map(x => (x._2))"
    // x._2 returns the second element of a tuple
    val lines = messages.map(_._2)

    // https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
     lines.foreachRDD(jsonRDD => {
       val data = spark.read.option("wholeFile", true).json(jsonRDD)
       if (data.count() > 0) {
         data.printSchema()
         // data.groupBy("stack_trace_01").count().show(upperLimit)
         import org.apache.spark.sql.functions._
         val resultSetByGroupBy = data.groupBy(
           log_level
           ,multi_thread_id
           ,stack_trace_01
           ,stack_trace_02
           ,stack_trace_03
           ,stack_trace_04
           ,stack_trace_05
           ,stack_trace_06
           ,stack_trace_07
           ,stack_trace_08
           ,stack_trace_09
           ,stack_trace_10
           ,stack_trace_11
           ,stack_trace_12
           ,stack_trace_13
           ,stack_trace_14).agg(min(message))
         resultSetByGroupBy.show(upperLimit) // OK
         data.show(upperLimit) // OK


         //data.createOrReplaceTempView("data")
         //resultSetByGroupBy.createOrReplaceTempView("resultSetByGroupBy")

         //val df = data.join(resultSetByGroupBy, data.col("stack_trace_01") === resultSetByGroupBy.col("stack_trace_01"), "inner")
         //val df = spark.sql("select * from data right join resultSetByGroupBy on data.stack_trace_01 == resultSetByGroupBy.stack_trace_01")

         // val df = resultSetByGroupBy.join(data, resultSetByGroupBy.col("stack_trace_01") === data.col("stack_trace_01"))
         //df.show(upperLimit)
//         resultSetByGroupBy.join(data)
//         resultSetByGroupBy.foreach(f => {
//           // data.show(upperLimit)
//           // '===' returns a column which contains the result of the comparisons of the elements of two columns
//           // val resultSetByFilter = data.filter($"stack_trace_01" === f.getAs("stack_trace_01"))
//           data.filter($"stack_trace_01" === f.getAs("stack_trace_01"))
//           // resultSetByFilter.show()
//         })
       }
       // data.createOrReplaceTempView("log")
       // val df = sqlContext.sql("SELECT * FROM log"

       /*resultSet.map(t => "Value: " + t(0)).collect().foreach(println)*/
    })

    /*val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCount.print()*/

    ssc.start()
    ssc.awaitTermination()

    /*System.setProperty("hadoop.home.dir", "/usr/local/share/hadoop/hadoop-2.7.3")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Log Aggregate Experiment")
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "YUI")
      .load()
    val schema: StructType = StructType(Seq(
      StructField("k",  StringType, true), StructField("v",  StringType, true)
    ))
    import spark.implicits._
    df.printSchema()
    df.select(from_json($"value".cast(StringType), schema))
    df.createOrReplaceTempView("LOG")
    val sqlDf = spark.sql("SELECT * FROM LOG")
    sqlDf
      .writeStream
      .format("console")
      .start()*/
  }
}
