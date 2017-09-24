package jp.ac.keio.sdm.OutliersDetectingExperiment

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ryuichi on 9/21/2017 AD.
  */
object OutliersDetectingExperiment {

  val ThreadCount = 2
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "OutliersDetectingExperiment"
  val BatchDuration = 2

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
    val lines = messages.map(_._2)

    //
    lines.foreachRDD(jsonRDD => {
      // Creates a DataFrame based on the content of a JSON RDD
      val df = spark.read.option("wholeFile", true).json(jsonRDD)
      val hashingTF = new HashingTF()
      val tf = hashingTF.transform(df)
      // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
      // First to compute the IDF vector and second to scale the term frequencies by IDF
      tf.cache()
      val idf = new IDF().fit(tf)
      val tfidf = idf.transform(tf)
      tfidf.foreach(x => println(x))
    })
  }

}