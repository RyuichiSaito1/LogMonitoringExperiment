package jp.ac.keio.sdm.OutliersDetectingExperiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ryuichi on 9/21/2017 AD.
  */
object OutliersDetectingExperiment {

  val ThreadCount = 2
  val SparkUrl = "local[" + ThreadCount + "]"
  val ApplicationName = "OutliersDetectingExperiment"
  val BatchDuration = 15
  // val SavingDirectoryForSampleData = "s3://aws-logs-757020086170-us-west-2/logs/error_sample"
  val SavingDirectoryForSampleData = "logs/error_sample"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(SparkUrl).setAppName(ApplicationName)
    val ssc = new StreamingContext(sparkConf, Seconds(BatchDuration))
    val spark = SparkSession
      .builder()
      .appName(ApplicationName)
      .getOrCreate()

    val errorFileDF = spark.read.parquet(SavingDirectoryForSampleData)
    errorFileDF.createOrReplaceTempView("errorFile")
    val errorFileTV = spark.sql("SELECT message FROM errorFile")
    import spark.implicits._
    errorFileTV.map(attributes => "message: " + attributes(0)).show()

    // Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).
    val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")
    val wordsData = tokenizer.transform(errorFileDF)

    // Compute Term Frequency.
    // Design NumFeatures.
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show()

    // Compute TFD-IDF.
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()

    // Design setK and setSeed
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(rescaledData)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    // WSSSE?
    val WSSSE = model.computeCost(rescaledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val transformedData = model.transform(rescaledData)
    transformedData.show()

    val centroids = model.clusterCenters
    // Define threshold of anomalies detection.
    // Need org.apache.spark.ml.linalg.Vector
    val threshold = transformedData.
      select("prediction", "features").as[(Int, Vector)].
      map{ case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec)}.
      orderBy($"value".desc).take(100).last

    val originalCols = rescaledData.columns
    val anomalies = transformedData.filter { row =>
      val cluster = row.getAs[Int]("prediction")
      val vec = row.getAs[Vector]("features")
      Vectors.sqdist(centroids(cluster), vec) >= threshold
    }.select(originalCols.head, originalCols.tail:_*)

    val anomaly = anomalies.first()
    val sentence = anomaly.getAs[String]("message")
    println(sentence)

    deleteDirectoryRecursively(new File(SavingDirectoryForSampleData))

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