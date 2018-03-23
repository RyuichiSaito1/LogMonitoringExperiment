/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */
package jp.ac.keio.sdm.AnomalyDetectingExperiment

import java.util.Properties

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model.{CreateTopicRequest, PublishRequest, SubscribeRequest}

class AmazonSNS {

  val properties = new Properties()
  properties.load(getClass.getResourceAsStream("/AwsCredentials.properties"))
  val accessKey = properties.getProperty("accessKey")
  val secretKey = properties.getProperty("secretKey")

  // create a new SNS client and set endpoint
  // Could not execute ClasspathPropertiesFileCredentialsProvider at Amazon EMR.
  // val snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider)
  val snsClient = new AmazonSNSClient(new BasicAWSCredentials(accessKey, secretKey))
  snsClient.setRegion(Region.getRegion(Regions.US_EAST_2))

  //create a new SNS topic
  // val createTopicRequest = new CreateTopicRequest("jp.ac.keio.sdm.AnomalyDetector")
  val createTopicRequest = new CreateTopicRequest("Londinium")
  val createTopicResult = snsClient.createTopic(createTopicRequest).getTopicArn
  //print TopicArn
  System.out.println(createTopicResult);
  //get request id for CreateTopicRequest from SNS metadata
  System.out.println("CreateTopicRequest - " + snsClient.getCachedResponseMetadata(createTopicRequest));

  def sendMessage(protocol: String, endpoint: String, message: String) {
    //subscribe to an SNS topic
    val subRequest = new SubscribeRequest()
    subRequest.setTopicArn(createTopicResult)
    subRequest.setProtocol(protocol)
    subRequest.setEndpoint(endpoint)
    snsClient.subscribe(subRequest)
    //get request id for SubscribeRequest from SNS metadata//get request id for SubscribeRequest from SNS metadata
    System.out.println("SubscribeRequest - " + snsClient.getCachedResponseMetadata(subRequest))
    System.out.println("Check your email and confirm subscription.")

    //publish to an SNS topic
    val publishRequest = new PublishRequest()
    publishRequest.setTopicArn(createTopicResult)
    publishRequest.setMessage(message)
    val publishResult = snsClient.publish(publishRequest)
    //print MessageId of message published to SNS topic
    System.out.println("MessageId - " + publishResult.getMessageId());
  }
}
