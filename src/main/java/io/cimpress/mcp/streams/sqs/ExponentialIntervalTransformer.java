package io.cimpress.mcp.streams.sqs;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

final class ExponentialIntervalTransformer<T> implements Observable.Transformer<T, Long> {


  private static final Logger LOG = LoggerFactory.getLogger(ExponentialIntervalTransformer.class);

  private long counter;

  private BackoffPolicy<T> backoffPolicy;

  public ExponentialIntervalTransformer(BackoffPolicy<T> backoffPolicy) {
    this.backoffPolicy = backoffPolicy;
    reset();
  }

  public BackoffPolicy<T> getBackoffPolicy() {
    return backoffPolicy;
  }

  public void setBackoffPolicy(BackoffPolicy<T> backoffPolicy) {
    this.backoffPolicy = backoffPolicy;
  }

  public void reset() {
    counter = 1L;
  }

  @Override
  public Observable<Long> call(Observable<T> observable) {
    return observable.flatMap(t -> {
      int backoff = 0;
      BiConsumer<String, Object> logMethod = LOG::debug;
      if (t instanceof EmptyQueueException) {
        backoff = backoffPolicy.getSleepDuration();
      } else if (backoffPolicy.shouldDelay(t)) {
        backoff = (int) Math.min(backoffPolicy.getMaxDelay(), Math.pow(backoffPolicy.getInitialDelay(), counter++));
        logMethod = LOG::warn;
      }

      logMethod.accept("Backing off observable for " + backoff + " seconds", t);
      return Observable.timer(backoff, TimeUnit.SECONDS, Schedulers.io())
          .doOnCompleted(() -> LOG.debug("Observable backoff completed"));
    });
  }
}
