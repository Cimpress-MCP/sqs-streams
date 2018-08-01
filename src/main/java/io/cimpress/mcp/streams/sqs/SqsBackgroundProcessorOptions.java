package io.cimpress.mcp.streams.sqs;


import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.ObjectMapper;
import rx.Completable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class SqsBackgroundProcessorOptions<INPUT> {

  String name;
  Function<INPUT, Completable> targetFn;
  Class<INPUT> inputType;
  AmazonSQS client;
  ObjectMapper mapper;
  Integer maxWait = 20;
  Integer maxMessages = 10;
  Integer timeout = 120;
  Optional<String> dlqName = Optional.empty();
  Integer delaySeconds = 0;
  Integer maxRetries = 10;
  Integer retryBaseDelaySeconds = 3600;
  Map<String, String> attributes = new HashMap<>();


  Optional<BackoffPolicy<Throwable>> backOffPolicy = Optional.empty();

  public SqsBackgroundProcessorOptions(String name, Function<INPUT, Completable> targetFn, Class<INPUT> inputType) {
    this.name = name;
    this.targetFn = targetFn;
    this.inputType = inputType;
  }

  public static <INPUT> SqsBackgroundProcessorOptions<INPUT> fromFunction(String name, Function<INPUT, ?> fn, Class<INPUT> inputType) {
    Function<INPUT, Completable> wrapFn = (item) -> {
      return Completable.fromAction(() -> fn.apply(item));
    };
    return new SqsBackgroundProcessorOptions<INPUT>(name, wrapFn, inputType);
  }

  public SqsBackgroundProcessorOptions<INPUT> withClient(AmazonSQS client) {
    this.client = client;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withMapper(ObjectMapper mapper) {
    this.mapper = mapper;
    return this;
  }


  public SqsBackgroundProcessorOptions<INPUT> withWaitTimeSeconds(Integer maxWait) {
    this.maxWait = maxWait;
    return this;
  }


  public SqsBackgroundProcessorOptions<INPUT> withMaxNumberOfMessages(Integer maxMessages) {
    this.maxMessages = maxMessages;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withVisibilityTimeout(Integer timeout) {
    this.timeout = timeout;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withDlqName(String dlqName) {
    this.dlqName = Optional.of(dlqName);
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withDelaySeconds(Integer delay) {
    this.delaySeconds = delay;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withBackOffPolicy(BackoffPolicy<Throwable> backOffPolicy) {
    this.backOffPolicy = Optional.of(backOffPolicy);
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withMaxRetries(Integer maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withRetryBaseDelaySeconds(Integer delay) {
    this.retryBaseDelaySeconds = delay;
    return this;
  }

  public SqsBackgroundProcessorOptions<INPUT> withAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
    return this;
  }
}
