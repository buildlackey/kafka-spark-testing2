package com.lackey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;


public abstract class AbstractKafkaSinkTest {
  final Logger logger = LoggerFactory.getLogger(AbstractKafkaSinkTest.class);

  protected static final String topic = "some-testTopic-" + new Date().getTime();
  protected static final int kafkaBrokerListenPort = 6666;

  KafkaContext kafkaCtx;

  abstract KafkaContext getKafkaContext() throws ExecutionException, InterruptedException;

  @BeforeTest
  public void setup() throws Exception {
    kafkaCtx = getKafkaContext();
    Thread.sleep(1000);           // TODO - try reducing time, or eliminating
  }

  public void testSparkReadingFromKafkaTopic() throws Exception {
    KafkaProducer<String, String> producer =  kafkaCtx.getProducer();
    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "dummy", "Hello, world");
    producer.send(producerRecord);

    SparkSession session = startNewSession();
    Dataset<Row> rows = session.read()
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:" + kafkaCtx.getBrokerListenPort())
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load();

    rows.printSchema();

    List<Row> rowList = rows.collectAsList();
    Row row = rowList.get(0);
    String strKey = new String(row.getAs("key"), "UTF-8");
    String strValue = new String(row.getAs("value"), "UTF-8");
    assert(strKey.equals("dummy"));
    assert(strValue.equals("Hello, world"));
  }

  SparkSession startNewSession() {
    SparkSession session = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
    session.sqlContext().setConf("spark.sql.shuffle.partitions", "1");   // cuts a few seconds off execution time
    return session;
  }
}

