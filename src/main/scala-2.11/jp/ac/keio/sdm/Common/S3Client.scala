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

  def doesObjectExist(bucketName: String, objcetName: String): Boolean = {
    return s3Client.doesObjectExist(bucketName, objcetName)
  }

  def deleteObject(bucketName: String, key: String) {
    s3Client.deleteObject(bucketName, key)
  }

}
