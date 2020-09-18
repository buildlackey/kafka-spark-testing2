package com.lackey;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;


public class ContainerizedKafkaSinkTest extends AbstractKafkaSinkTest {
  KafkaContext getKafkaContext() throws ExecutionException, InterruptedException {
    return new ContainerizedKafkaContext(topic, 9092);
  }

  @Test
  public void testSparkReadingFromKafkaTopic() throws Exception {
    super.testSparkReadingFromKafkaTopic();
  }
}

