package io.cimpress.mcp.streams.sqs;

import com.amazonaws.services.sqs.model.Message;
import rx.Observable;

public interface StreamConsumer<T> {

  Observable<Committable<Message>> start();

}
