package io.cimpress.mcp.streams.sqs;

import org.joda.time.DateTime;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class LatchAnswer<T> implements Answer<T> {

  Logger LOG = LoggerFactory.getLogger(LatchAnswer.class);

  private CountDownLatch latch;
  private Answer<T> answer;

  public LatchAnswer(CountDownLatch latch, Answer<T> answer) {
    this.latch = latch;
    this.answer = answer;
  }

  public LatchAnswer(CountDownLatch latch, T result) {
    this.latch = latch;
    this.answer = x -> result;
  }

  @Override
  public T answer(InvocationOnMock invocation) throws Throwable {
    try {
      LOG.error("Answering with " + answer.getClass().getSimpleName() + " at " + DateTime.now());
      return answer.answer(invocation);
    } finally {
      LOG.error("Counting down " + latch.getCount() + " at " + DateTime.now());
      latch.countDown();
    }
  }
}
