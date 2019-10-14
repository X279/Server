package com.amazonaws.simples.Server;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public interface SQS {
	static AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	static String QueueName = "myQueue";
	static String URL = sqs.getQueueUrl(SQS.QueueName).getQueueUrl();
	static String QueueName1 = "myQueue1";
	static String URL1 = sqs.getQueueUrl(SQS.QueueName1).getQueueUrl();
}
