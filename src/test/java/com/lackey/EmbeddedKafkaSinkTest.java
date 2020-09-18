package com.lackey;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;


public class EmbeddedKafkaSinkTest extends AbstractKafkaSinkTest {
  KafkaContext getKafkaContext() throws ExecutionException, InterruptedException {
    return new EmbeddedKafkaContext(topic, kafkaBrokerListenPort);
  }


  @Test
  public void testSparkReadingFromKafkaTopic() throws Exception {
    super.testSparkReadingFromKafkaTopic();
  }
}

