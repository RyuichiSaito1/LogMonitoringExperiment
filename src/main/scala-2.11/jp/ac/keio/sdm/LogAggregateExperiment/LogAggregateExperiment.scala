package jp.ac.keio.sdm.LogAggregateExperiment

import io.netty.handler.codec.string
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import kafka.serializer.StringDecoder

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Created by Ryuichi on 6/2/2017 AD.
  */
object LogAggregateExperiment {

  final val threadCount = 2
  final val applicationName = "LogAggregateExperiment"
  final val batchDuration = 2

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
      .getOrCreate()

    import spark.implicits._

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
       data.printSchema()
       data.show()
       // data.groupBy("message").count().show()
       data.createOrReplaceTempView("log")
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
