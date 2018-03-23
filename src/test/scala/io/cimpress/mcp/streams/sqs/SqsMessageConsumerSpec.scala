package io.cimpress.mcp.streams.sqs

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import com.amazonaws.services.sqs.AmazonSQS
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import rx.lang.scala.JavaConversions._
import rx.schedulers.Schedulers

import scala.concurrent.duration._

class SqsMessageConsumerSpec() extends TestKit(ActorSystem("SqsMessageConsumerSpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  val mapper: ObjectMapper = new ObjectMapper()
  var client: AmazonSQS = null

  override def beforeAll {
    AbstractSqsTest.start()
    client = AbstractSqsTest.configuration.newClient()

  }

  override def afterAll {
    AbstractSqsTest.stop()
    TestKit.shutdownActorSystem(system)
  }

  def createConsumer(name: String): SqsMessageConsumer = {
    val options = createDefaultMessageConsumerOptions();
    createConsumer(name, options)
  }

  def createDefaultMessageConsumerOptions(): SqsMessageConsumerOptions = {
    new SqsMessageConsumerOptions().withWaitTimeSeconds(1)
      .withMaxNumberOfMessages(1).withVisibilityTimeout(2);
  }

  def createConsumer(name: String, options: SqsMessageConsumerOptions): SqsMessageConsumer = {
    new SqsMessageConsumer(client, "sqsconsumer-" + name, options)
  }

  case class Event(body: String)

  "A message" must {

    "be received on subscribe" in {
      val consumer = createConsumer("testSubscribe");
      val event = Event("test")
      val o = toScalaObservable(consumer.start())
      val echo = system.actorOf(TestActors.echoActorProps)

      o.subscribeOn(Schedulers.io()).subscribe(msg => {
        msg.commit();
        val result = msg.getValue.getBody
        echo ! result
      });

      consumer.createProducer(mapper).send(event).subscribe();

      expectMsg(mapper.writeValueAsString(event))
    }

    "not be received again after commit" in {
      val consumer = createConsumer("testSubscribeOnce");
      val event = Event("test")
      val o = toScalaObservable(consumer.start())
      val echo = system.actorOf(TestActors.echoActorProps)

      o.subscribeOn(Schedulers.io()).subscribe(msg => {
        msg.commit();
        val result = msg.getValue.getBody
        echo ! result
      });

      consumer.createProducer(mapper).send(event).subscribe();

      expectMsgClass(classOf[String])

      expectNoMsg(3 seconds)
    }

    "be received again after visibility times out" in {
      val consumer = createConsumer("testSubscribeFailure");
      val event = Event("test")
      val o = toScalaObservable(consumer.start())

      val subscription = o.subscribeOn(Schedulers.io()).subscribe(msg => {
        val result = msg.getValue.getBody
        testActor ! result
      });

      consumer.createProducer(mapper).send(event).subscribe();

      expectMsgClass(classOf[String])

      expectMsgClass(classOf[String])
      subscription.unsubscribe();
    }

    "not be received again when a consumer is configured to sleep after reading a message" in {
      val sleepTime = 10;
      val waitTime = sleepTime - 2;
      val options = createDefaultMessageConsumerOptions().withVisibilityTimeout(sleepTime - 6)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {
          @Override
          def getSleepDuration: Int = {
            return sleepTime;
          }
        });

      val consumer = createConsumer("testSubscribeOnceWithSleep", options);
      val event = Event("test")
      val o = toScalaObservable(consumer.start())
      val echo = system.actorOf(TestActors.echoActorProps)

      o.subscribeOn(Schedulers.io()).subscribe(msg => {
        val result = msg.getValue.getBody
        echo ! result
      });

      consumer.createProducer(mapper).send(event).subscribe();

      expectMsgClass(classOf[String])

      expectNoMsg(waitTime seconds)
    }

    "be received again when a consumer is configured to sleep after reading a message, wake up, and read the message again" in {
      val sleepTime = 2;
      val waitTime = sleepTime + 6;
      val options = createDefaultMessageConsumerOptions().withVisibilityTimeout(sleepTime + 2)
        .withBackOffPolicy(new BackoffPolicy[Throwable]() {
          @Override
          def getSleepDuration: Int = {
            return sleepTime;
          }
        });

      val consumer = createConsumer("testSubscribeOnceWithSleepRetry", options);
      val event = Event("test")
      val o = toScalaObservable(consumer.start())

      o.subscribeOn(Schedulers.io()).subscribe(msg => {
        val result = msg.getValue.getBody
        testActor ! result
      });

      consumer.createProducer(mapper).send(event).subscribe();

      expectMsgClass(classOf[String])

      expectMsgClass(waitTime seconds, classOf[String])
    }
  }
}
