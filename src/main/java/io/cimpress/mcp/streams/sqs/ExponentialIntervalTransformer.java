package io.cimpress.mcp.streams.sqs;


import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

final class ExponentialIntervalTransformer<T extends Throwable> implements Observable.Transformer<T, Long> {


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
      TriConsumer<Marker, String, Throwable> logMethod = LOG::debug;
      if (t instanceof EmptyQueueException) {
        backoff = backoffPolicy.getSleepDuration();
      } else if (backoffPolicy.shouldDelay(t)) {
        backoff = (int) Math.min(backoffPolicy.getMaxDelay(), Math.pow(backoffPolicy.getInitialDelay(), counter++));
        logMethod = LOG::warn;
      }

      Marker marker;
      if (t instanceof Marked) {
        marker = Marked.class.cast(t).getMarker();
      } else {
        marker = Markers.append("underlyingMessage", t.getMessage());
      }

      logMethod.accept(marker, "Backing off observable for " + backoff + " seconds", t);
      return Observable.timer(backoff, TimeUnit.SECONDS, Schedulers.io())
          .doOnCompleted(() -> LOG.debug("Observable backoff completed"));
    });
  }

  @FunctionalInterface
  private interface TriConsumer<T, V, U> {
    void accept(T param1, V param2, U param3);
  }
}
