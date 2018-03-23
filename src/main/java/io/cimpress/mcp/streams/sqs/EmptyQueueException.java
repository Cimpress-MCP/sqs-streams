package io.cimpress.mcp.streams.sqs;

public class EmptyQueueException extends RuntimeException {

  public EmptyQueueException(String message) {
    super(message);
  }
}
