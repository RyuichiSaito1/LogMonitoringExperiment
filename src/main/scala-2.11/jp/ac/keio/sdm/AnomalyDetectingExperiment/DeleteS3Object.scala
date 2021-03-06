/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */

/*
Reference to https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/aws/example/s3/DeleteObjects.java
 */
package jp.ac.keio.sdm.AnomalyDetectingExperiment

import java.util

import com.amazonaws.{AmazonServiceException, ClientConfiguration, Protocol}
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectsRequest

/**
  * Delete multiple objects from an Amazon S3 bucket.
  *
  * This code expects that you have AWS credentials set up per:
  * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
  *
  * ++ Warning ++ This code will actually delete the objects that you specify!
  */
class DeleteS3Object {

  val TimeOutMilliSeconds = 10000
  val URL = "https://s3.console.aws.amazon.com"
  val Region = "us-west-2"

  def deleteS3Objcet(args: Array[String]) {

    val USAGE = "\n" + "To run this component, supply the name of an S3 bucket and at least\n" + "one object name (key) to delete.\n" + "\n" + "Ex: DeleteObjects <bucketname> <objectname1> [objectname2, ...]\n"
    if (args.length < 2) {
      System.out.println(USAGE)
      System.exit(1)
    }

    val bucket_name = args(0)
    val object_keys = util.Arrays.copyOfRange(args, 1, args.length).toString
    System.out.println("Deleting objects from S3 bucket: " + bucket_name)
    for (k <- object_keys) {
      System.out.println(" * " + k)
    }

    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTPS)
    clientConfig.setConnectionTimeout(TimeOutMilliSeconds)

    val endpointConfiguration = new EndpointConfiguration(URL, Region)

    val s3 = AmazonS3ClientBuilder.standard()
      .withCredentials(new ClasspathPropertiesFileCredentialsProvider)
      .withClientConfiguration(clientConfig)
      .withEndpointConfiguration(endpointConfiguration).build()

    try {
      val dor = new DeleteObjectsRequest(bucket_name).withKeys(object_keys)
      s3.deleteObjects(dor)
    } catch {
      case e: AmazonServiceException =>
        System.err.println(e.getErrorMessage)
        System.exit(1)
    }
    System.out.println("Done!")
  }
}