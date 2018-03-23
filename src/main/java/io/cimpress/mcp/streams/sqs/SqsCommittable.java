package io.cimpress.mcp.streams.sqs;

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.function.Function;

public class SqsCommittable implements Committable<Message> {

  private static Logger LOG = LoggerFactory.getLogger(SqsCommittable.class);

  private Message value;
  private Function<?, ?> commitFn;

  public SqsCommittable(Message msg, Function<?, ?> commitFn) {
    this.value = msg;
    this.commitFn = commitFn;
  }

  @Override
  public Message getValue() {
    return value;
  }

  @Override
  public void commit() {
    Observable.fromCallable(() -> commitFn.apply(null))
        .subscribeOn(Schedulers.io())
        .observeOn(Schedulers.computation())
        .doOnError(e -> {
          Marker marker = SqsUtils.buildMarker(value, e);
          LOG.error(marker, "failed to delete from SQS");
        })
        .retry(3)
        .subscribe();
  }

}
