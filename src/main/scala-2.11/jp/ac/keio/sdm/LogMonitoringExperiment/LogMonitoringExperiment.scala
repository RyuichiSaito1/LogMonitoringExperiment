package jp.ac.keio.sdm.LogMonitoringExperiment

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ryuichi on 5/5/2017 AD.
  */
object LogMonitoringExperiment {

  final val threadCount = 2
  final val batchDuration = 2

  // args example: localhost:2181 test YUI 2
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum,group,topics,numThreads) = args
    val sparkUrl = "local[" + threadCount + "]"
    val sparkConf = new SparkConf().setMaster(sparkUrl).setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    kafkaStream.foreachRDD { line =>
      var isContents = false
      println("Hello World")
      println(line)
      if (line.collect().contains("EXP-") || isContents) {
        println(line)
        val filteredLine = line
        isContents = true
        filteredLine.saveAsTextFile("output/errorLog")
      } else {
        isContents = false
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}