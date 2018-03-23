package io.cimpress.mcp.streams.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.marker.LogstashMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class SqsRetryWorker<INPUT> {

  private static final Logger LOG = LoggerFactory.getLogger(SqsRetryWorker.class);

  public static String RETRY_Q_POSTFIX = "RETRY";

  private SqsMessageConsumer consumer;

  private Subscription sub;

  private ObjectMapper mapper;

  private Class<INPUT> inputType;

  private SqsMessageProducer processorQProducer;

  private SqsMessageProducer producer;

  private SqsBackgroundProcessorOptions<INPUT> options;

  private String processorQName;


  public SqsRetryWorker(String processorQName, SqsBackgroundProcessorOptions<INPUT> options, SqsMessageProducer processorQProducer) {
    this.processorQName = processorQName;
    this.options = options;
    this.mapper = options.mapper;
    this.inputType = options.inputType;
    this.processorQProducer = processorQProducer;
  }

  private static void traceMessage(final String msg, final Message message) {
    LogstashMarker marker = SqsUtils.buildMarker(message);
    LOG.trace(marker, msg);
  }

  public void stop() {
    this.sub.unsubscribe();
  }

  public void start() {
    SqsMessageConsumerOptions consumerOptions = new SqsMessageConsumerOptions().withVisibilityTimeout(options.retryBaseDelaySeconds)
        .withMaxNumberOfMessages(options.maxMessages)
        .withMaxReads(options.maxRetries)
        .withWaitTimeSeconds(options.maxWait);
    this.options.dlqName.map(dlqName -> consumerOptions.withDlqName(dlqName));
    this.consumer = new SqsMessageConsumer(options.client, SqsUtils.buildQueueName(processorQName, RETRY_Q_POSTFIX), consumerOptions);
    this.producer = this.consumer.createProducer(mapper);
    Observable<Object> stream = consumer.start()
        .doOnError(t -> LOG.error("Error reading retry message", t))
        .flatMap((final Committable<Message> item) -> {
          Message message = item.getValue();
          String receiveCountString = message.getAttributes().get("ApproximateReceiveCount");
          int receiveCount = Integer.parseInt(receiveCountString);
          TaskDetails<INPUT> task = SqsUtils.extractTask(mapper, message, inputType);
          Completable chain = Completable.complete();
          // if we have received this message more times than the number of attempts, then requeue
          // to the task q. Else, do nothing and don't re-read until the visibility timeout.
          if (receiveCount > task.getAttempts()) {
            chain = chain.andThen(processorQProducer.send(task).toCompletable())
                .andThen(Completable.fromAction(() -> {
                  traceMessage("Sent back for retry", message);
                  item.commit();
                }));
          } else {
            traceMessage("Delaying retry", message);
          }

          return chain.toObservable();
        })
        .doOnError(t -> LOG.error("Error queueing retry", t))
        .retry();

    this.sub = stream
        .subscribeOn(Schedulers.io())
        .observeOn(Schedulers.computation())
        .subscribe(
            nothing -> LOG.trace("Retry complete"),
            t -> LOG.error("Error retrying message", t));

  }

  public SqsMessageProducer getProducer() {
    return this.producer;
  }

}
