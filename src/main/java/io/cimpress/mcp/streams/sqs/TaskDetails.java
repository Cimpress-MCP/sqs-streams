package io.cimpress.mcp.streams.sqs;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class TaskDetails<I> {

  private String taskName;
  private int attempts = 0;
  private I inputData;
  private String lastError;
  private boolean successful = true;
  private Long lastAttempted;

  public TaskDetails() {

  }

  public TaskDetails(String taskName, I inputData) {
    super();
    this.taskName = taskName;
    this.inputData = inputData;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public int getAttempts() {
    return attempts;
  }

  public void setAttempts(int attempts) {
    this.attempts = attempts;
  }

  public I getInputData() {
    return inputData;
  }

  public void setInputData(I inputData) {
    this.inputData = inputData;
  }

  public void addAttempt() {
    this.attempts++;
    this.lastAttempted = DateTime.now(DateTimeZone.UTC).getMillis();
  }

  public void addFailure(Throwable exception) {
    this.successful = false;
    this.lastAttempted = DateTime.now(DateTimeZone.UTC).getMillis();
    this.lastError = exception.getMessage();
  }

  public String getLastError() {
    return lastError;
  }

  public void setLastError(String lastError) {
    this.lastError = lastError;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public void setSuccessful(boolean successful) {
    this.successful = successful;
  }

  public Long getLastAttempted() {
    return lastAttempted;
  }

  public void setLastAttempted(Long lastAttempted) {
    this.lastAttempted = lastAttempted;
  }


}
