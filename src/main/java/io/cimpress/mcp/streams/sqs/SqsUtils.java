package io.cimpress.mcp.streams.sqs;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.marker.LogstashMarker;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class SqsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SqsUtils.class);

  public static String buildQueueName(String... parts) {
    String name = String.join("-", parts);
    return name;
  }

  public static String getQueueUrl(AmazonSQS client, String queueName) {
    GetQueueUrlResult queueNameResult = client.getQueueUrl(queueName);
    return queueNameResult.getQueueUrl();
  }

  public static String getQueueArn(AmazonSQS client, String queueUrl) {
    // Get the ARN
    GetQueueAttributesRequest dlqAttributeRequest = new GetQueueAttributesRequest(queueUrl).withAttributeNames("All");
    String arn = client.getQueueAttributes(dlqAttributeRequest).getAttributes().get("QueueArn");
    return arn;
  }


  public static LogstashMarker buildMarker(Message message) {
    return buildMarker(message, null);
  }

  public static LogstashMarker buildMarker(Message message, Throwable thrown) {

    LogstashMarker marker = Markers.append("messageId", message.getMessageId());

    if (thrown != null) {
      marker.add(Markers.append("exception", thrown));
    }

    return marker;

  }

  public static <INPUT> TaskDetails<INPUT> extractTask(ObjectMapper mapper, final Message message, Class<INPUT> inputType) {
    TaskDetails<INPUT> task;
    try {
      JavaType type = mapper.getTypeFactory().constructParametricType(TaskDetails.class, inputType);
      task = mapper.readValue(message.getBody(), type);
    } catch (IOException e) {
      throw new RuntimeException("Unable to deserialize task", e);
    }
    if (task.getInputData() == null) {
      throw new RuntimeException("Unable to deserialize task: input data is null");
    }
    return task;
  }
}
