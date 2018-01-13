package jp.ac.keio.sdm.OutliersDetectingExperiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ryuichi on 9/21/2017 AD.
  */
object OutliersDetectingExperiment {

  val ThreadCount = "*"
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "OutliersDetectingExperiment"
  val BatchDuration = 15
  val S3BacketName = "s3://aws-logs-757020086170-us-west-2"
  // Development Mode.
  // val SavingDirectoryForSampleData = "data/parquet"
  // Product Mode.
  val SavingDirectoryForSampleData = "s3://aws-logs-757020086170-us-west-2/elasticmapreduce/data/parquet"
  val KSize = 3
  val SeedSize = 1L
  val UpperLimit = 10000

  def main(args: Array[String]) {

    // Development Mode.
    // val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    // Product Mode.
    val sparkConf = new SparkConf().setAppName(ApplicationName)
    val ssc = new StreamingContext(sparkConf, Seconds(BatchDuration))
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
    val analysingDF = spark.sql("SELECT CONCAT(analysedMessage, ' ', analysedStackTrace01, ' ', analysedStackTrace02) AS messages FROM errorFile")
    analysingDF.show()
    // import spark.implicits._
    // analysingDF.map(attributes => "messages: " + attributes(0)).show()

    // Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).
    val tokenizer = new Tokenizer()
      .setInputCol("messages").setOutputCol("words")
    val wordsData = tokenizer.transform(analysingDF)

    // Compute Term Frequency.
    // Design NumFeatures.
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(UpperLimit)

    // Compute TF-IDF.
    val idf = new IDF()
      .setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(UpperLimit)

    //dataPoint.show(UpperLimit)
    //rescaledData.rdd.coalesce(1, true).saveAsTextFile("logs/error_sample2")

    //rescaledData.select("features").rdd.map(_.getAs[SparseVector](0).values).take(2)
    //rescaledData.select("features").rdd.map(_.getAs[SparseVector](0).toDense).saveAsTextFile("logs/error_sample2")
    //val labeled = rescaledData.map(row => LabeledPoint(row.getDouble(0), row.getAs[SparseVector](3).toDense))
    //labeled.rdd.coalesce(1, true).saveAsTextFile("logs/error_sample3")
    //labeled.rdd.coalesce(1, true).saveAsTextFile("s3://aws-logs-757020086170-us-west-2/logs/error_sample2")

    // Design setK and setSeed
    val kmeans = new KMeans()
      .setK(KSize).setSeed(SeedSize)
    val model = kmeans.fit(rescaledData)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    // WSSSE?
    val WSSSE = model.computeCost(rescaledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val transformedData = model.transform(rescaledData)
    transformedData.show(UpperLimit)

    val centroids = model.clusterCenters
    // Define threshold of anomalies detection.
    // Need org.apache.spark.ml.linalg.Vector
    import spark.implicits._
    val threshold = transformedData.
      select("prediction", "features").as[(Int, Vector)].
      map{ case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec)}.
      orderBy($"value".desc).take(5).last

    val originalCols = rescaledData.columns
    val anomalies = transformedData.filter { row =>
      val cluster = row.getAs[Int]("prediction")
      val vec = row.getAs[Vector]("features")
      Vectors.sqdist(centroids(cluster), vec) >= threshold
    }.select(originalCols.head, originalCols.tail:_*)

    val anomaly = anomalies.first()
    anomalies.show(UpperLimit)
    val sentence = anomaly.getAs[String]("messages")
    println(sentence)

    // Development Mode.
    //deleteDirectoryRecursively(new File(SavingDirectoryForSampleData))
    // Product Mode.
    val deleteS3Objcet = new DeleteS3Object
    deleteS3Objcet.deleteS3Objcet(Array(S3BacketName, SavingDirectoryForSampleData))

    // $example off$
    spark.stop()
  }

  def deleteDirectoryRecursively(file: File) {
    if (file.isDirectory)
      file.listFiles.foreach(deleteDirectoryRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Could not delete ${file.getAbsolutePath}")
  }
}