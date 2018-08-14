/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */
package jp.ac.keio.sdm.AnomalyDetectingExperiment

import java.util.Properties

import jp.ac.keio.sdm.Common.S3Client
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.floor

object AnomalyDetectingExperiment {

  val ThreadCount = "*"
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "OutliersDetectingExperiment"
  val S3BacketName = "aws-logs-757020086170-us-west-2"
  val S3ObjectName = "elasticmapreduce/data_20180814/parquet"

  // Development Mode.
  // val SavingDirectoryForAggregateData = "data/number_parquet"
  // Product Mode.
  // val SavingDirectoryForAggregateData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data/parquet"
  val SavingDirectoryForAggregateData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data_20180814/parquet"

  // Result that word's hashcode divided NumFeatures is mapped NumFeatures size.
  val NumFeatureSize = 10000
  // Execution frequency for choosing initial cluster centroid positions( or seeds).
  val SeedSize = 10L
  val UpperLimit = 10000
  val PartitionNum = 1

  def main(args: Array[String]) {

    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/AwsEndpoints.properties"))
    val MSN = properties.getProperty("MSN")
    val EMail = properties.getProperty("EMail")

    // Development Mode.
    val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    val sc = new SparkContext(sparkConf)
    // Product Mode.
    // val sparkConf = new SparkConf().setAppName(ApplicationName)
    // val sc = new SparkContext(sparkConf)
    val s3Client = new S3Client
    val isObjcetExist = s3Client.objcetList(S3BacketName,S3ObjectName)

    if (isObjcetExist == true) {

      val spark = SparkSession
        .builder()
        .appName(ApplicationName)
        .getOrCreate()
      // if (new File(SavingDirectoryForSampleData).exists == false){ return }
      val aggregateErrorFileDF = spark.read.parquet(SavingDirectoryForAggregateData)
      // val schema = spark.read.parquet(SavingDirectoryForSampleData).schema
      // val errorFileDF = spark.readStream.schema(schema).parquet(SavingDirectoryForSampleData)
      // Create multiplex messages Dataframe.
      val multiplexDF = aggregateErrorFileDF.filter(aggregateErrorFileDF("Number") > 1)
      multiplexDF.createOrReplaceTempView("multiplexView")
      val notifyingMultiplexDF = spark.sql("SELECT CONCAT(date_time, ' ', log_level, ' ', multi_thread_id, ' ', message, ' ', stack_trace_01, ' ', stack_trace_02) AS messages FROM multiplexView")
      val multiplexList = notifyingMultiplexDF.select("messages").collectAsList()
      // Define k-means size.
      val criterionNumber = multiplexDF.count().toInt
      val temporaryKSize = criterionNumber / 2
      val KSize = floor(temporaryKSize).toInt

      var messages = ""
      // Get multiplex messages.
      for (i <- 0 to criterionNumber - 1) {
        messages = messages + "\r\r".concat(multiplexList.get(i).toString())
      }

      if (KSize > 1) {
        // Create clustering messages Dataframe except for multiplex messages.
        val errorFileDF = spark.read.parquet(SavingDirectoryForAggregateData)
        val analysedMessageDF = errorFileDF.filter(errorFileDF("Number") < 2).withColumn("analysedMessage", regexp_replace(errorFileDF("message"), "\\.", " "))
        val analysedStackTrace01DF = analysedMessageDF.withColumn("analysedStackTrace01", regexp_replace(errorFileDF("stack_trace_01"), "\\.", " "))
        val analysedDF = analysedStackTrace01DF.withColumn("analysedStackTrace02", regexp_replace(errorFileDF("stack_trace_02"), "\\.", " "))
        analysedDF.createOrReplaceTempView("errorFile")
        val analysingDF = spark.sql("SELECT CONCAT(date_time, ' ', log_level, ' ', multi_thread_id, ' ', message, ' ', stack_trace_01, ' ', stack_trace_02) AS messages" +
          ", CONCAT(analysedMessage, ' ', analysedStackTrace01, ' ', analysedStackTrace02) AS analysedMessages FROM errorFile")
        analysingDF.show()
        // import spark.implicits._
        // analysingDF.map(attributes => "messages: " + attributes(0)).show()

        // Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).
        val tokenizer = new Tokenizer()
          .setInputCol("analysedMessages").setOutputCol("words")
        val wordsData = tokenizer.transform(analysingDF)

        // Compute Term Frequency.
        val hashingTF = new HashingTF()
          .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(NumFeatureSize)
        val featurizedData = hashingTF.transform(wordsData)
        featurizedData.show(UpperLimit)

        // Compute TF-IDF.
        val idf = new IDF()
          .setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featurizedData)
        val rescaledData = idfModel.transform(featurizedData)
        rescaledData.show(UpperLimit)

        // Design setK and setSeed
        val kmeans = new KMeans()
          .setK(KSize).setSeed(SeedSize)
        val model = kmeans.fit(rescaledData)

        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        val WSSSE = model.computeCost(rescaledData)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Shows the result.
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)

        val transformedData = model.transform(rescaledData)
        transformedData.show(UpperLimit)

        // Calculate Squared Distance.
        val centroids = model.clusterCenters
        val squaredDistance = (cluster: Int, datapoint: Vector) => {
          Vectors.sqdist(centroids(cluster), datapoint)
        }
        import org.apache.spark.sql.functions._
        val CalculateSquaredDistance = udf(squaredDistance)
        val squredDistanceData = transformedData.withColumn("square_distance", CalculateSquaredDistance(col("prediction"), col("features"))).distinct()
        squredDistanceData.show(UpperLimit)

        val predeictionData = squredDistanceData.groupBy("prediction").agg(min("square_distance"))
        predeictionData.show(UpperLimit)
        val renamedPredictionData = predeictionData.withColumnRenamed("min(square_distance)", "square_distance")
        val finalData = squredDistanceData.join(renamedPredictionData, Seq("prediction", "square_distance"))
        finalData.show(UpperLimit)
        //val messages = finalData.select("messages").collectAsList().toString
        // Get clustering messages.
        for (i <- 0 to KSize - 1) {
          if (finalData.groupBy("prediction").count().filter(finalData("prediction") === i) == 0) return
          val temporaryData = finalData.filter(finalData("prediction") === i).select("messages").first().toString()
          messages = messages + "\r\r".concat(temporaryData)
        }
      }

      var sendingNumber = 0
      if (KSize > 1) {
        sendingNumber = criterionNumber + KSize
      } else {
        sendingNumber = criterionNumber
      }

      System.out.println("String =" + messages)
      val finalMessages = "Hello, I'm a Anomalies Detector.\rWe just have detected application failures.\rPlease check following " + sendingNumber + " messages and stack traces." + messages
      println(finalMessages)

      val amazonSNS = new AmazonSNS();
      // amazonSNS.sendMessage("sms", MSN, finalMessages)
      amazonSNS.sendMessage("email", EMail, finalMessages)

      s3Client.deleteObject(S3BacketName, S3ObjectName)
    } else {
      System.out.println("Object did not exist in S3 Bucket.")
    }

    // Development Mode.
    // deleteDirectoryRecursively(new File(SavingDirectoryForAggregateData))
    // Product Mode.
    // val deleteS3Objcet = new DeleteS3Object
    // deleteS3Objcet.deleteS3Objcet(Array(S3BacketName, SavingDirectoryForAggregateData))
    sc.stop()
  }

  /*def deleteDirectoryRecursively(file: File) {
    if (file.isDirectory)
      file.listFiles.foreach(deleteDirectoryRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Could not delete ${file.getAbsolutePath}")
  }*/
}
