package io.cimpress.mcp.streams.sqs

import java.net.InetAddress
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.sqs.AmazonSQSClient
import org.elasticmq.NodeAddress
import org.elasticmq.rest.sqs.{SQSRestServer, SQSRestServerBuilder}
import rx.lang.scala.JavaConversions._

class SqsClientSpec extends UnitSpec {


  val queueName = s"regression-${System.currentTimeMillis}-${InetAddress.getLocalHost.getHostName}"
  val queueUrl = new AtomicReference[String](null)


  val sqsPort = 19000 + ThreadLocalRandom.current().nextInt(0, 1000);
  val sqsHost = "localhost";

  val server = SQSRestServerBuilder.withPort(sqsPort).withServerAddress(new NodeAddress("http", sqsHost, sqsPort, ""))

  val creds = new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"));

  val client = new AmazonSQSClient(creds)
  client.setEndpoint(s"http://${sqsHost}:${sqsPort}")
  val testMessage = "test message body"
  var serverProc: SQSRestServer = null;

  override def beforeAll() {
    serverProc = server.start()
  }

  override def afterAll() {
    serverProc.stopAndWait()
  }

  "A Queue" should "be created" in {

    val options = new SqsMessageConsumerOptions().withWaitTimeSeconds(1).withMaxNumberOfMessages(1).withVisibilityTimeout(2);
    val consumer = new SqsMessageConsumer(client, "sqsconsumer-create", options);
    val r = toScalaObservable(
      consumer.start()
    ).subscribe();
    queueUrl.set(consumer.getQueueUrl)
    assert(Option(queueUrl.get).isDefined)
    SqsUtils.getQueueArn(client, queueUrl.get)
  }
}
