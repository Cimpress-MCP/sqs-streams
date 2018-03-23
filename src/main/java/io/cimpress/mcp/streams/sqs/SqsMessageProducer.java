package io.cimpress.mcp.streams.sqs;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.schedulers.Schedulers;


public class SqsMessageProducer {

  private static final Logger LOG = LoggerFactory.getLogger(SqsMessageProducer.class);

  private String queueUrl;

  private AmazonSQS client;

  private ObjectMapper mapper;


  public SqsMessageProducer(final AmazonSQS client, final String queueUrl, ObjectMapper mapper) {
    this.client = client;
    this.queueUrl = queueUrl;
    this.mapper = mapper;
  }


  public Single<SendMessageResult> send(Object source) {
    String payload = convertToText(source);
    return sendJson(payload);
  }

  public Single<SendMessageResult> sendJson(String payload) {
    final SendMessageRequest sendMessageRequest = new SendMessageRequest(this.queueUrl, payload);
    return Single.fromCallable(() -> client.sendMessage(sendMessageRequest)).subscribeOn(Schedulers.io())
        .observeOn(Schedulers.trampoline());
  }

  private String convertToText(Object source) {
    try {
      return mapper.writeValueAsString(source);
    } catch (JsonProcessingException e) {
      LOG.error("Unable to serialize object", e);
      throw new RuntimeException(e);
    }
  }

}
