package io.cimpress.mcp.streams.sqs

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.amazonaws.services.sqs.AmazonSQS
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._

class SqsBackgroundProcessorSpec() extends TestKit(ActorSystem("SqsBackgroundProcessorSpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with Eventually {

  val mapper: ObjectMapper = new ObjectMapper()
  val log = Logging(system, classOf[SqsBackgroundProcessorSpec])
  var client: AmazonSQS = null

  override def beforeAll {
    AbstractSqsTest.start()
    client = AbstractSqsTest.configuration.newClient()

  }

  override def afterAll {
    AbstractSqsTest.stop()
    TestKit.shutdownActorSystem(system)
  }

  def createOptions(name: String, fn: Function[String, _]): SqsBackgroundProcessorOptions[String] = {
    return SqsBackgroundProcessorOptions.fromFunction(name, asJavaFunction(fn), classOf[String])
      .withClient(client)
      .withMapper(mapper)
      .withWaitTimeSeconds(1)
      .withMaxNumberOfMessages(1)
      .withRetryBaseDelaySeconds(1)
      .withVisibilityTimeout(3);
  }

  def createProcessor(options: SqsBackgroundProcessorOptions[String]): SqsBackgroundProcessor[String] = {
    val processor = new SqsBackgroundProcessor[String](options);
    processor.start();
    return processor;
  }

  case class Event(body: String)


  "A background processor" must {

    "process an input" in {

      def fn(s: String) = {
        testActor ! s
      }

      val options = createOptions("testProcess", fn _);
      val processor = createProcessor(options);

      processor.process("hello").await()

      expectMsgClass(classOf[String])

    }

    "not commit when retry fails" in {


      val probe = TestProbe()

      @volatile var returnError = true

      @volatile var count = 1

      def fn(s: String) = {
        if (returnError) {
          count += 1
          throw new RuntimeException("error" + count)
        }
        probe.ref ! s
      }

      val options = createOptions("testNoCommitOnRetryError", fn _).withRetryBaseDelaySeconds(1)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {

          @Override
          def shouldDelay(e: Throwable): Boolean = {
            log.error("NOT Backing off for testNoCommitOnRetryError " + e.getMessage());
            return false;
          }

        });
      val processor = createProcessor(options);

      import org.mockito.Matchers._
      import org.mockito.Mockito._
      val producer = mock(classOf[SqsMessageProducer])
      when(producer.send(any())).thenThrow(new RuntimeException("retry failed"))
      processor.setRetryProducer(producer)
      processor.process("don't commit retry").await()

      probe.expectNoMsg(10 seconds)

      log.error("Returning success now for testNoCommitOnRetryError");
      returnError = false
      probe.expectMsgClass(3 seconds, classOf[String])
      verify(producer, org.mockito.Mockito.atLeast(2)).send(any())

    }

    "backoff processing on errors" in {

      @volatile var count = 1

      def fn(s: String) = {
        count += 1
        throw new RuntimeException("backoff" + count)
      }

      val options = createOptions("testBackoff", fn _).withRetryBaseDelaySeconds(3600);
      val processor = createProcessor(options);

      Range(1, 20).foreach(i => {
        processor.process("backoff " + i).await()
      });
      expectNoMsg(10 seconds)

    }

    "reset error backoff on success" in {

      @volatile var returnError = true

      @volatile var count = 1

      def fn(s: String) = {
        if (returnError) {
          count += 1
          throw new RuntimeException("backoff" + count)
        }
        testActor ! s
      }

      val options = createOptions("testBackoffWithReset", fn _).withRetryBaseDelaySeconds(3600);
      val processor = createProcessor(options);

      Range(1, 20).foreach(i => {
        processor.process("testBackoffWithReset " + i).await()
      });
      expectNoMsg(10 seconds)

      returnError = false
      expectMsgClass(30 seconds, classOf[String])

    }


    "not backoff for client errors" in {

      @volatile var returnError = true

      @volatile var count = 1

      def fn(s: String) = {
        if (returnError) {
          count += 1
          throw new RuntimeException("error" + count)
        }
        testActor ! s
      }

      val options = createOptions("testNoBackoff", fn _).withRetryBaseDelaySeconds(3600)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {

          @Override
          def shouldDelay(e: Throwable): Boolean = {
            log.error("NOT Backing off for testNoBackoff " + e.getMessage());
            return false;
          }

        });
      val processor = createProcessor(options);

      Range(1, 20).foreach(i => {
        processor.process("testNoBackoff " + i).await()
      });

      returnError = false
      expectMsgClass(3 seconds, classOf[String])

    }

    "retry on error" in {


      val probe = TestProbe()

      @volatile var returnError = true

      @volatile var count = 1

      def fn(s: String) = {
        if (returnError) {
          count += 1
          throw new RuntimeException("error" + count)
        }
        probe.ref ! s
      }

      val options = createOptions("testRetryOnError", fn _).withRetryBaseDelaySeconds(1)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {

          @Override
          def shouldDelay(e: Throwable): Boolean = {
            log.error("NOT Backing off for testRetryOnError " + e.getMessage());
            return false;
          }

        });
      val processor = createProcessor(options);

      Range(1, 20).foreach(i => {
        processor.process("testRetryOnError " + i).await()
      });
      probe.expectNoMsg(10 seconds)

      log.error("Returning success now for testRetryOnError");
      returnError = false
      probe.expectMsgClass(3 seconds, classOf[String])

    }


    "retry up to max attempts configuration" in {

      implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)),
        interval = scaled(Span(500, Millis)));

      val probe = TestProbe()


      @volatile var count = 1
      val MAX_ATTEMPTS = 5

      def fn(s: String) = {
        count += 1
        if (count > MAX_ATTEMPTS) probe.ref ! s
        throw new RuntimeException("error" + count)
      }

      val options = createOptions("testRetryMaxAttempts", fn _).withRetryBaseDelaySeconds(1)
        .withMaxRetries(MAX_ATTEMPTS)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {

          @Override
          def shouldDelay(e: Throwable): Boolean = {
            log.error("NOT Backing off for testRetryOnError " + e.getMessage());
            return false;
          }

        });
      val processor = createProcessor(options);

      processor.process("testRetryMaxAttempts").await()

      eventually {
        count should be(MAX_ATTEMPTS)
      }

      probe.expectNoMsg()
    }

    "not retry on deserialization error" in {


      val probe = TestProbe()

      def fn(s: String) = {
        probe.ref ! s
      }

      val options = createOptions("testNoRetryOnDerSerError", fn _);
      val processor = createProcessor(options);

      processor.process(null).await();
      probe.expectNoMsg(10 seconds)


      processor.process("hello").await()

      probe.expectMsgClass(classOf[String])
    }

  }


}
