package io.cimpress.mcp.streams.sqs;

import rx.Completable;

@FunctionalInterface
public interface BackgroundProcessor<INPUT> {

  public Completable process(final INPUT input);

}
