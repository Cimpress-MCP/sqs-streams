package io.cimpress.mcp.streams.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.elasticmq.NodeAddress;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class AbstractSqsTest {

  public static String prefix;
  public static SQSRestServer server;
  public static int sqsPort = 19000 + ThreadLocalRandom.current().nextInt(0, 1000);
  public static String sqsHost = "localhost";
  public static SqsConfiguration configuration;

  public AbstractSqsTest() {
    super();
  }

  @BeforeClass
  public static void start() throws Exception {
    prefix = "events-junit";
    server = SQSRestServerBuilder.withPort(sqsPort).withServerAddress(new NodeAddress("http", sqsHost, sqsPort, "")).start();
    configuration = new SqsConfiguration();
    configuration.setEndpoint(Optional.of("http://" + sqsHost + ":" + sqsPort));
    configuration.setCredentialsProvider(Optional.of(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secret"))));
  }

  @AfterClass
  public static void stop() throws Exception {
    Thread.sleep(200);
    server.stopAndWait();
  }
}
