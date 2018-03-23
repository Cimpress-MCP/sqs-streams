package io.cimpress.mcp.streams.sqs;

public interface Committable<T> {

  void commit();

  T getValue();

}
