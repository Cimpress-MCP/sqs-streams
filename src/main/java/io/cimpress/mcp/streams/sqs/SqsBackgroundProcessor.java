package io.cimpress.mcp.streams.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.marker.LogstashMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.function.Function;

public class SqsBackgroundProcessor<INPUT> implements BackgroundProcessor<INPUT> {


  private static final Logger LOG = LoggerFactory.getLogger(SqsBackgroundProcessor.class);

  private Function<INPUT, Completable> targetFn;

  private ObjectMapper mapper;

  private SqsMessageConsumer consumer;

  private Subscription sub;

  private ExponentialIntervalTransformer<Throwable> exponentialBackOff = new ExponentialIntervalTransformer<>(new BackoffPolicy<Throwable>() {
  });

  private Class<INPUT> inputType;

  private SqsMessageProducer producer;

  private AmazonSQS client;

  private String name;

  private SqsBackgroundProcessorOptions<INPUT> options;

  private SqsRetryWorker<INPUT> retryWorker;

  private SqsMessageProducer retryProducer;

  public SqsBackgroundProcessor(SqsBackgroundProcessorOptions<INPUT> options) {
    this.options = options;
    this.name = options.name;
    this.targetFn = options.targetFn;
    this.inputType = options.inputType;
    this.client = options.client;
    this.mapper = options.mapper;
  }

  public void stop() {
    this.sub.unsubscribe();
    this.retryWorker.stop();
  }

  public Completable process(final INPUT input) {
    TaskDetails<INPUT> task = new TaskDetails<>(this.name, input);
    return producer.send(task).onErrorResumeNext(e -> {
      if (e instanceof AmazonClientException) {
        e = new RuntimeException("Unable to create background task on SQS", e);
      }
      return Single.error(e);
    }).toCompletable();
  }

  public void start() {
    options.backOffPolicy.ifPresent(policy -> this.exponentialBackOff.setBackoffPolicy(policy));
    SqsMessageConsumerOptions consumerOptions = new SqsMessageConsumerOptions().withDelaySeconds(options.delaySeconds)
        .withMaxNumberOfMessages(options.maxMessages)
        .withVisibilityTimeout(options.timeout)
        .withMaxReads(options.maxRetries)
        .withWaitTimeSeconds(options.maxWait);
    this.options.dlqName.map(dlqName -> consumerOptions.withDlqName(dlqName));
    this.consumer = new SqsMessageConsumer(this.client, this.name, consumerOptions);
    this.producer = consumer.createProducer(mapper);
    this.retryWorker = createRetryWorker();
    this.retryWorker.start();
    this.retryProducer = this.retryWorker.getProducer();

    Observable<Committable<Message>> stream = consumer.start()
        .subscribeOn(Schedulers.io())
        .observeOn(Schedulers.io());
    Observable<Committable<Message>> tasks = stream.flatMap((final Committable<Message> item) -> {
      try {
        final TaskDetails<INPUT> task = SqsUtils.extractTask(mapper, item.getValue(), inputType);
        task.addAttempt();
        return processInput(item, task)
            .toSingleDefault(item)
            .toObservable();
      } catch (Throwable t) {
        LOG.error("Error reading background task", t);
        item.commit();
        return Observable.just(item);
      }


    })
        .retryWhen(errors -> errors.compose(exponentialBackOff))
        .doOnNext(n -> exponentialBackOff.reset());

    this.sub = tasks.subscribe(
        msg ->
        {
          LogstashMarker marker = SqsUtils.buildMarker(msg.getValue());
          LOG.debug(marker, "Completed task");
        },
        t -> LOG.error("Fatal error running background task", t));

  }

  /**
   * Process the input data in a deferred fashion to capture and retry exceptions.
   *
   * @param item being processed
   * @param task details with input data
   * @return {@link Completable}
   */
  private Completable processInput(final Committable<Message> item, final TaskDetails<INPUT> task) {
    return Completable.defer(() -> targetFn.apply(task.getInputData()))
        .onErrorResumeNext(t -> {
          task.addFailure(t);
          return this.retryProducer.send(task).toCompletable()
              .doOnError(e -> {
                LogstashMarker marker = SqsUtils.buildMarker(item.getValue());
                LOG.error(marker, "Failed to send to retry queue", e);
              })
              .andThen(Completable.fromAction(() -> item.commit()))
              .andThen(Completable.error(t))
              .onErrorResumeNext(e -> Completable.error(t));

        })
        .andThen(Completable.fromAction(() -> item.commit()));
  }

  private SqsRetryWorker<INPUT> createRetryWorker() {

    return new SqsRetryWorker<>(this.name, this.options, this.producer);
  }

  public void setRetryProducer(SqsMessageProducer retryProducer) {
    this.retryProducer = retryProducer;
  }


}
