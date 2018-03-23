package io.cimpress.mcp.streams.sqs;

public interface BackoffPolicy<T> {
  public static final int INITIAL_DELAY = 2;
  public static final int MAX_DELAY = Integer.MAX_VALUE;
  public static final int SLEEP_DURATION = 0;

  public default int getInitialDelay() {
    return INITIAL_DELAY;
  }

  public default int getMaxDelay() {
    return MAX_DELAY;
  }

  public default boolean shouldDelay(T error) {
    return true;
  }

  public default int getSleepDuration() {
    return SLEEP_DURATION;
  }
}
