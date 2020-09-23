package com.lackey;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ContainerizedKafkaContext implements KafkaContext {
  final Logger logger = LoggerFactory.getLogger(ContainerizedKafkaContext.class);
  private final int kafkaBrokerListenPort;


  ContainerizedKafkaContext(String topic, int kafkaBrokerListenPort) throws ExecutionException, InterruptedException {
    this.kafkaBrokerListenPort =  kafkaBrokerListenPort;

    AdminClient adminClient = AdminClient.create(getKafkaProperties());

    NewTopic newTopic = new NewTopic(topic, (short)1, (short)1);
    ImmutableList<NewTopic> topicList = ImmutableList.of(newTopic);
    CreateTopicsResult result = adminClient.createTopics(topicList);

    KafkaFuture<Void> future = result.all();

    // Wait until done. Nothing stops this from waiting forever, so we should make build time out with error .
    future.get();
  }

  @Override
  public int getBrokerListenPort() {
    return 9092;
  }
}
