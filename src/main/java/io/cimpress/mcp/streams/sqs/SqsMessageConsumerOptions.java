package io.cimpress.mcp.streams.sqs;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SqsMessageConsumerOptions {

  Integer maxWait = 20;
  Integer maxMessages = 10;
  Integer timeout = 120;
  Optional<String> dlqName = Optional.empty();
  Integer delaySeconds = 0;
  Integer maxReads = 10;
  Optional<BackoffPolicy<Throwable>> backOffPolicy = Optional.empty();
  Map<String, String> attributes = new HashMap<>();

  public SqsMessageConsumerOptions withWaitTimeSeconds(Integer maxWait) {
    this.maxWait = maxWait;
    return this;
  }

  public SqsMessageConsumerOptions withMaxNumberOfMessages(Integer maxMessages) {
    this.maxMessages = maxMessages;
    return this;
  }

  public SqsMessageConsumerOptions withVisibilityTimeout(Integer timeout) {
    this.timeout = timeout;
    return this;
  }

  public SqsMessageConsumerOptions withDlqName(String dlqName) {
    this.dlqName = Optional.of(dlqName);
    return this;
  }

  public SqsMessageConsumerOptions withDelaySeconds(Integer delay) {
    this.delaySeconds = delay;
    return this;
  }

  public SqsMessageConsumerOptions withMaxReads(Integer maxReads) {
    this.maxReads = maxReads;
    return this;
  }

  public SqsMessageConsumerOptions withBackOffPolicy(BackoffPolicy<Throwable> backOffPolicy) {
    this.backOffPolicy = Optional.of(backOffPolicy);
    return this;
  }

  public SqsMessageConsumerOptions withAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
    return this;
  }
}
