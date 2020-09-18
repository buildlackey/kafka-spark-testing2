package com.lackey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

public class EmbeddedKafkaContext implements KafkaContext {
  final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaContext.class);
  private final int kafkaBrokerListenPort;
  private final String bootStrapServers;
  private final String zookeeperConnectionString;

  EmbeddedKafkaContext(String topic, int kafkaBrokerListenPort) {
    this.kafkaBrokerListenPort =  kafkaBrokerListenPort;

    EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, false, topic);
    broker.kafkaPorts(kafkaBrokerListenPort);
    broker.afterPropertiesSet();
    zookeeperConnectionString = broker.getZookeeperConnectionString();
    bootStrapServers = broker.getBrokersAsString();
    logger.info("zookeeper: {}, bootstrapServers: {}", zookeeperConnectionString, bootStrapServers);
  }

  @Override
  public int getBrokerListenPort() {
    return kafkaBrokerListenPort;
  }
}
