package io.cimpress.mcp.streams.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.util.Optional;

public class SqsConfiguration {

  private String region;
  private Optional<String> endpoint = Optional.empty();
  private String queuePrefix;
  private Integer workers = 50;
  private Optional<Integer> defaultRetryDelaySeconds = Optional.empty();
  private Optional<Integer> defaultPollSeconds = Optional.empty();
  private Optional<AWSCredentialsProvider> credentialsProvider;

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getQueuePrefix() {
    return queuePrefix;
  }

  public void setQueuePrefix(String queuePrefix) {
    this.queuePrefix = queuePrefix;
  }

  public Integer getWorkers() {
    return workers;
  }

  public void setWorkers(Integer workers) {
    this.workers = workers;
  }

  public Optional<AWSCredentialsProvider> getCredentialsProvider() {
    return credentialsProvider;
  }

  public void setCredentialsProvider(Optional<AWSCredentialsProvider> credentialsProvider) {
    this.credentialsProvider = (credentialsProvider == null) ? Optional.empty() : credentialsProvider;
  }

  public AmazonSQS newClient() {
    final ClientConfiguration config = new ClientConfiguration().withMaxConnections(workers);
    AmazonSQSClientBuilder clientBuilder = AmazonSQSClient.builder();
    clientBuilder.setClientConfiguration(config);
    if (endpoint.isPresent()) {
      clientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint.get(), getRegion()));
    } else {
      clientBuilder.setRegion(getRegion());
    }

    credentialsProvider.ifPresent(clientBuilder::setCredentials);

    return clientBuilder.build();
  }

  public Optional<String> getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(Optional<String> endpoint) {
    this.endpoint = (endpoint == null) ? Optional.empty() : endpoint;
  }

  public Optional<Integer> getDefaultRetryDelaySeconds() {
    return this.defaultRetryDelaySeconds;

  }

  public void setDefaultRetryDelaySeconds(Optional<Integer> defaultRetryDelaySeconds) {
    this.defaultRetryDelaySeconds = (defaultRetryDelaySeconds == null) ? Optional.empty() : defaultRetryDelaySeconds;
  }

  public Optional<Integer> getDefaultPollSeconds() {
    return defaultPollSeconds;
  }

  public void setDefaultPollSeconds(Optional<Integer> defaultPollSeconds) {
    this.defaultPollSeconds = (defaultPollSeconds == null) ? Optional.empty() : defaultPollSeconds;
  }
}
