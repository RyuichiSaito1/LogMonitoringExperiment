/* Copyright (c) 2017 Ryuichi Saito, Keio University. All right reserved. */
package jp.ac.keio.sdm.AnomalyDetectingExperiment

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model.{CreateTopicRequest, PublishRequest, SubscribeRequest}

class AmazonSNS {

  // create a new SNS client and set endpoint
  val snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider)
  snsClient.setRegion(Region.getRegion(Regions.US_WEST_2))

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
