package jp.ac.keio.sdm.OutliersDetectingExperiment

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model.{CreateTopicRequest, PublishRequest, SubscribeRequest}

class AmazonSNS {

  // create a new SNS client and set endpoint
  val snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider)
  snsClient.setRegion(Region.getRegion(Regions.US_EAST_1))

  //create a new SNS topic//create a new SNS topic
  val createTopicRequest = new CreateTopicRequest("jp.ac.keio.sdm.AnomalyDetector")
  val createTopicResult = snsClient.createTopic(createTopicRequest)
  //print TopicArn
  System.out.println(createTopicResult);
  //get request id for CreateTopicRequest from SNS metadata
  System.out.println("CreateTopicRequest - " + snsClient.getCachedResponseMetadata(createTopicRequest));

  //subscribe to an SNS topic
  val subRequest = new SubscribeRequest()
  subRequest.setTopicArn(createTopicResult.toString)
  subRequest.setProtocol("sms")
  subRequest.setEndpoint("+818030956898")
  snsClient.subscribe(subRequest)
  //get request id for SubscribeRequest from SNS metadata//get request id for SubscribeRequest from SNS metadata
  System.out.println("SubscribeRequest - " + snsClient.getCachedResponseMetadata(subRequest))
  System.out.println("Check your email and confirm subscription.")

  //publish to an SNS topic
  val message = "My text published to SNS topic with email endpoint";
  val publishRequest = new PublishRequest()
  publishRequest.setTopicArn(createTopicResult.toString)
  publishRequest.setMessage(message)
  val publishResult = snsClient.publish(publishRequest)
  //print MessageId of message published to SNS topic
  System.out.println("MessageId - " + publishResult.getMessageId());
}
