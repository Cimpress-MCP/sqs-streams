package io.cimpress.mcp.streams.sqs;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.marker.LogstashMarker;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.observables.AsyncOnSubscribe;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;


public class SqsMessageConsumer implements StreamConsumer<Message> {

  private static final Logger LOG = LoggerFactory.getLogger(SqsMessageConsumer.class);
  private static final int MAX_VISIBILITY_TIMEOUT = 43200;

  private ExponentialIntervalTransformer<Throwable> exponentialBackOff = new ExponentialIntervalTransformer<Throwable>(new BackoffPolicy<Throwable>() {
  });

  private AmazonSQS client;
  private String queueUrl;

  private String queueName;

  private SqsMessageConsumerOptions options = new SqsMessageConsumerOptions();


  public SqsMessageConsumer(AmazonSQS client, String queueName) {
    this(client, queueName, new SqsMessageConsumerOptions());
  }

  public SqsMessageConsumer(AmazonSQS client, String queueName, SqsMessageConsumerOptions options) {
    this.client = client;
    this.queueName = queueName;
    this.options = options;
    this.queueUrl = createQueue(queueName, options);
    this.options.backOffPolicy.ifPresent(backoffPolicy -> this.exponentialBackOff.setBackoffPolicy(backoffPolicy));
  }

  @Override
  public Observable<Committable<Message>> start() {

    Observable<Committable<Message>> stream = Observable.create(new SqsAsyncSubscriber())
        .subscribeOn(Schedulers.io())
        .observeOn(Schedulers.computation())
        .filter(msg -> msg != null)
        .doOnError(e -> {
          LogstashMarker marker = Markers.append("queue", queueName);
          if (e instanceof EmptyQueueException) {
            LOG.info(marker, "No messages to read. SqsMessageConsumer is going to sleep", e);
          } else {
            LOG.error(marker, "Error retrieving messages from SQS", e);
          }
        })
        .retryWhen(o -> o.compose(exponentialBackOff))
        .doOnNext(item -> exponentialBackOff.reset())
        .map(msg -> (Committable<Message>) new SqsCommittable(msg, (options) -> delete(msg)));

    return stream;
  }

  private ReceiveMessageResult read(Integer max) {

    final ReceiveMessageRequest readRequest = new ReceiveMessageRequest(this.queueUrl)
        .withAttributeNames(MessageSystemAttributeName.ApproximateReceiveCount.toString(),
            MessageSystemAttributeName.ApproximateFirstReceiveTimestamp.toString())
        .withMaxNumberOfMessages(max)
        .withVisibilityTimeout(options.timeout)
        .withWaitTimeSeconds(options.maxWait);
    final ReceiveMessageResult result = client.receiveMessage(readRequest);
    return result;
  }

  private DeleteMessageResult delete(Message message) {
    final DeleteMessageRequest deleteRequest = new DeleteMessageRequest(this.queueUrl, message.getReceiptHandle());
    final DeleteMessageResult result = client.deleteMessage(deleteRequest);
    return result;
  }

  public ChangeMessageVisibilityResult changeVisibilityTimeout(Message message, int visibilityTimeout) {
    // safeguard to ensure we do not try to set the visibility timeout higher than the maximum allowed
    if (options.timeout + visibilityTimeout > MAX_VISIBILITY_TIMEOUT) {
      visibilityTimeout = MAX_VISIBILITY_TIMEOUT - options.timeout;
    }

    final ChangeMessageVisibilityRequest changeVisibilityResult =
        new ChangeMessageVisibilityRequest(this.queueUrl, message.getReceiptHandle(), visibilityTimeout);
    return client.changeMessageVisibility(changeVisibilityResult);
  }

  public String getQueueUrl() {
    return this.queueUrl;
  }

  private String createQueue(String queueName, SqsMessageConsumerOptions options) {
    CreateQueueResult result = client.createQueue(queueName);
    updateQueueAttributes(result.getQueueUrl(), options);
    return result.getQueueUrl();
  }

  public SqsMessageProducer createProducer(ObjectMapper mapper) {
    return new SqsMessageProducer(client, queueUrl, mapper);
  }

  private void updateQueueAttributes(String queueUrl, SqsMessageConsumerOptions options) {

    try {
      // Set RedrivePolicy for the original queue
      SetQueueAttributesRequest setQueueAttributesRequest =
          new SetQueueAttributesRequest().withQueueUrl(queueUrl)
              .addAttributesEntry("DelaySeconds", options.delaySeconds.toString())
              .addAttributesEntry("MessageRetentionPeriod", "1209600");
      options.dlqName.ifPresent(dlqName -> {
        String dlqUrl = SqsUtils.getQueueUrl(this.client, dlqName);
        String dlqArn = SqsUtils.getQueueArn(this.client, dlqUrl);
        setQueueAttributesRequest.addAttributesEntry("RedrivePolicy", formatRedrivePolicy(options.maxReads.toString(), dlqArn));
      });
      client.setQueueAttributes(setQueueAttributesRequest);

    } catch (Exception e) {
      LOG.error("Failed to update options for " + queueName + ".. moving on", e);
    }
  }

  private String formatRedrivePolicy(String... args) {
    return String.format("{\"maxReceiveCount\":\"%s\", \"deadLetterTargetArn\":\"%s\"}", args);
  }

  public String getQueueName() {
    return queueName;
  }

  private final class SqsAsyncSubscriber extends AsyncOnSubscribe<List<Message>, Message> {

    private int max = options.maxMessages;

    @Override
    protected List<Message> generateState() {
      return new ArrayList<>();
    }

    @Override
    protected List<Message> next(List<Message> state, long requested, Observer<Observable<? extends Message>> observer) {

      Observable<Message> nextBatch = Observable.fromCallable(() -> read(requested > max ? max : (int) requested))
          .subscribeOn(Schedulers.io())
          .observeOn(Schedulers.computation())
          .doOnError(e -> LOG.error("Failed to read from SQS " + queueName, e))
          .flatMap(result -> {
            List<Message> msgs = result.getMessages();
            if (msgs.isEmpty()) {
              return Observable.error(new EmptyQueueException("The queue is empty. Go to sleep"));
            }
            return Observable.from(msgs);
          });
      observer.onNext(nextBatch);
      return state;
    }

  }


}
