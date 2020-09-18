package com.lackey;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public interface KafkaContext {

  int getBrokerListenPort();

  default public KafkaProducer<String, String> getProducer() {
    return new KafkaProducer<>(getKafkaProperties());
  }

  default  Properties getKafkaProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:" + getBrokerListenPort());
    props.put("acks", "all");
    props.put("auto.create.topics.enable", "false");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
}
