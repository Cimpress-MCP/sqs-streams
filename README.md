# SQS Streams [![Build Status](https://travis-ci.org/Cimpress-MCP/sqs-streams.svg?branch=master)](https://travis-ci.org/Cimpress-MCP/sqs-streams)

SQS Streams is a low level library for consuming from AWS [Simple Queue Service](https://aws.amazon.com/sqs/).

It provides a reactive interface for consuming messages using [rxJava](http://reactivex.io/).

It also provides a higher level component for queueing data to be processed by a user-supplied function with retries and exponential backoff.

The BackgroundProcessor API and SQS implementation could be moved to their own projects at some point and only the low level components would stay in this one.

**BETA - Expect the API to change.  There are no compatibility guarantees between versions**

### Getting Started

Add sqs-streams as a dependency to your project.


``` xml
<dependency>
  <groupId>io.cimpress.mcp</groupId>
  <artifactId>sqs-streams</artifactId>
  <version>0.4.0</version>
</dependency>
```


### Usage

Currently tests are the best way to understand how to use this library in your project.

#### SqsMessageConsumer and SqsMessageProducer

The SqsMessageConsumer implementation provides a reactive API for consuming SQS messages.  It also provides an SqsMessageProducer to enqueue the messages. This low-level API is used in the BackgroundProcessor but can also be used directly for other use cases.

```java
SqsMessageConsumerOptions<String> options = new SqsMessageConsumerOptions()
      .withVisibilityTimeout(30);

SqsMessageConsumer consumer = new SqsMessageConsumer(client, qName, options);
consumer.start().subscribe( msg -> System.out.println(msg.getBody() + "world");
SqsMessageProducer producer = consumer.createProducer(mapper);
producer.send("hello").await();
```


#### BackgroundProcessor

There is an SQS implementation of the BackgroundProcessor interface to enable durably queueing data to be processed asynchronously with retries and exponential backoff.

```java
SqsBackgroundProcessorOptions<String> options = SqsBackgroundProcessorOptions.fromFunction("mytask", (input) -> System.out.println(input + "world"), String.class)
      .withClient(sqsclient)
      .withMapper(objectmapper)
      .withRetryBaseDelaySeconds(30)
      .withVisibilityTimeout(30);

SqsBackgroundProcessor<String> processor = new SqsBackgroundProcessor<>(options);
processor.start();

processor.process("hello").await(); //Don't forget to subscribe or await to the Completable result
```
