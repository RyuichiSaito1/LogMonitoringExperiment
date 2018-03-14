/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */
package jp.ac.keio.sdm.AnomalyDetectingExperiment

import java.io.File

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.{SparkConf, SparkContext}

object AnomalyDetectingExperiment {

  val ThreadCount = "*"
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "OutliersDetectingExperiment"
  val S3BacketName = "s3://aws-logs-757020086170-us-west-2"
  // Development Mode.
  val SavingDirectoryForSampleData = "data/parquet"
  // Product Mode.
  // val SavingDirectoryForSampleData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data/parquet"
  // Result that word's hashcode divided NumFeatures is mapped NumFeatures size.
  val NumFeatureSize = 10000
  val KSize = 3
  // Execution frequency for choosing initial cluster centroid positions( or seeds).
  val SeedSize = 10L
  val UpperLimit = 10000
  val PartitionNum = 1
  val MSN = "+818030956898"

  val SavingDirectoryForFinalData = "data/text/final"

  def main(args: Array[String]) {

    // Development Mode.
    val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    // Product Mode.
    // val sparkConf = new SparkConf().setAppName(ApplicationName)
    val sc = new SparkContext(sparkConf)

    val spark = SparkSession
      .builder()
      .appName(ApplicationName)
      .getOrCreate()
    //if (new File(SavingDirectoryForSampleData).exists == false){ return }
    val errorFileDF = spark.read.parquet(SavingDirectoryForSampleData)

    val analysedMessageDF = errorFileDF.withColumn("analysedMessage", regexp_replace(errorFileDF("message"), "\\.", " "))
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

    val centroids = model.clusterCenters
    import spark.implicits._
    val reprentative = transformedData.
      select("prediction", "features").as[(Int, Vector)].
      map{ case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec)}.
      orderBy($"value".asc)
    reprentative.show(UpperLimit)

    val squaredDistance = (cluster: Int, datapoint: Vector) => {
      Vectors.sqdist(centroids(cluster), datapoint)
    }
    import org.apache.spark.sql.functions._
    val CalculateSquaredDistance = udf(squaredDistance)
    val squredDistanceData = transformedData.withColumn("square_distance", CalculateSquaredDistance(col("prediction"), col("features"))).distinct()
    squredDistanceData.show(UpperLimit)
    val predeictionData = squredDistanceData.groupBy("prediction").agg(min("square_distance"))
    val renamedPredictionData = predeictionData.withColumnRenamed("min(square_distance)", "square_distance")
    val finalData = squredDistanceData.join(renamedPredictionData, Seq("prediction","square_distance"))
    finalData.show(UpperLimit)
    val messages = finalData.select("messages").collectAsList().toString
    val finalMessages = "Hello, I'm a Anomalies Detector, We are sending you three representative messages.\nPlease check following messages and stack traces.\n\n" + messages.replace(",","\n\nMessage").replace("[[","[Message[")
    println(finalMessages)

    val amazonSNS = new AmazonSNS();
    amazonSNS.sendMessage("sms", MSN, finalMessages)

    // Development Mode.
    // deleteDirectoryRecursively(new File(SavingDirectoryForSampleData))
    // Product Mode.
    // val deleteS3Objcet = new DeleteS3Object
    // deleteS3Objcet.deleteS3Objcet(Array(S3BacketName, SavingDirectoryForSampleData))
    sc.stop()
  }

  def deleteDirectoryRecursively(file: File) {
    if (file.isDirectory)
      file.listFiles.foreach(deleteDirectoryRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Could not delete ${file.getAbsolutePath}")
  }
}
