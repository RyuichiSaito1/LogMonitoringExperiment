package jp.ac.keio.sdm.Common

import java.util.Properties

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

class S3Client {

  val properties = new Properties()
  properties.load(getClass.getResourceAsStream("/AwsCredentials.properties"))
  val accessKey = properties.getProperty("accessKey")
  val secretKey = properties.getProperty("secretKey")
  val credentials = new BasicAWSCredentials(accessKey, secretKey)

  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
    .withRegion(Regions.US_WEST_2)
    .build()

  def objcetList(bucketName: String, objcetName: String): Boolean = {
    val objcetList = s3Client.listObjects(bucketName, objcetName)
    val list = objcetList.getObjectSummaries
    if (list.size() > 0) {
      true
    } else {
      false
    }
  }

  def deleteObject(bucketName: String, key: String) {
    //s3Client.deleteObject(bucketName, key)
    import scala.collection.JavaConversions._
    for (file <- s3Client.listObjects(bucketName, key).getObjectSummaries) {
      s3Client.deleteObject(bucketName, file.getKey)
    }
  }

}
