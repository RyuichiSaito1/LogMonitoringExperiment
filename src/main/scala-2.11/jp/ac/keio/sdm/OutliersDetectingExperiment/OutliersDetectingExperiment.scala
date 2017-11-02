package jp.ac.keio.sdm.OutliersDetectingExperiment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
// import org.apache.spark.mllib.feature.{HashingTF, IDF}

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

    /*implicit class FileMonads(f: File) {
      def check = if (f.exists) Some(f) else None //returns "Maybe" monad
      def remove = if (f.delete()) Some(f) else None //returns "Maybe" monad
    }*/

    val errorFileDF = spark.read.parquet("output/parquet")
    errorFileDF.cache()
    deleteRecursively(new File("output/parquet"))
    /*for {
      foundFile <- new File("logs/error_sample/._SUCCESS.crc").check
      deletedFile <- foundFile.remove
    } yield deletedFile*/

    errorFileDF.createOrReplaceTempView("errorFile")
    val errorFileTV = spark.sql("SELECT message FROM errorFile")
    import spark.implicits._
    errorFileTV.map(attributes => "message: " + attributes(0)).show()

  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }







  /*val ThreadCount = 2
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
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder
      .appName("TfIdfExample")
      .getOrCreate()
    /*val ssc = new StreamingContext(sc, Seconds(BatchDuration))
    val spark = SparkSession
      .builder()
      .appName(ApplicationName)
      // .config("spark.some.config.option", "some-value")
    .getOrCreate()*/

    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sentenceData = spark.createDataFrame(Seq(
        (0, "Hi I heard about Spark"),
        (0, "I wish Java could use case classes"),
        (1, "Logistic regression models are neat"),
        (1, "I love Scala very much"),
        (1, "Regular regression models are cool")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features").take(3).foreach(println)
    rescaledData.show()
    // rescaledData.select("features", "label").write.format("csv").save("output")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(rescaledData)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(rescaledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val transformedData = model.transform(rescaledData)
    transformedData.show()

    val centroids = model.clusterCenters
    import spark.implicits._
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
    val sentence = anomaly.getAs[String]("sentence")

    println(sentence)

    // $example off$

    spark.stop()*/


    /*val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)

    // Generate RDD from the stream
    lines.foreachRDD(jsonRDD => {
      // Creates a DataFrame based on the content of a JSON RDD
      val df = spark.read.option("wholeFile", true).json(jsonRDD)
      if (df.count() > 0) {
        val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")
        val wordsData = tokenizer.transform(df)
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("tf")
        val tf = hashingTF.transform(wordsData)
        // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
        // First to compute the IDF vector and second to scale the term frequencies by IDF
        tf.cache()
        val idf = new IDF().setInputCol("tf").setOutputCol("tf-idf").fit(tf)
        val tfidf = idf.transform(tf).cache()
        // tfidf.show()
        println("Hello YUI")
        tfidf.select("tf-idf", "label").take(3).foreach(println)
        // tfidf.foreach(x => println(x))

        /*val kmeans = new KMeans().setK(3).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
        val model = kmeans.fit(tfidf)

        val WSSSE = model.computeCost(tfidf)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Shows the result.
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)*/
      }
    })*/

    /*ssc.start()
    ssc.awaitTermination()*/

}