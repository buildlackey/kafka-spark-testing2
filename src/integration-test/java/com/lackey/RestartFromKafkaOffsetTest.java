package com.lackey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This test starts a pipeline twice, the second time re-reading one record from the last offset in one particular
 * partition. We verify that the totals contain exactly what is expected (that is, .... DESCRIBE RESULTS)
 *<pre>
 * If you ran the commands below:
 *
 *   num_events=11
 *   topic=another-testTopic-1591068423012         ## Note - the topic is randomized per test.
 *   kafkacat -b localhost:9092 -t $topic -C -c$num_events  -f '\nKey (%K bytes): %k\t\nValue (%S bytes): \
 *                    %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'
 *
 * You would see this record at offset three of the partition we are re-reading (all other are ignored in second pass):
 *
 * Key (12 bytes): key: [ some key value ]
 * Value (66 bytes):  [ some value ]
 * Timestamp: 2222222222222        Partition: 1    Offset: [some offset ]
 *</pre>
 *
 *
 **/
public class RestartFromKafkaOffsetTest {

  private String startingOffsets = "earliest";    // This is setting for first run, then overriden w/ values from config

  public SparkSession session;


  final Logger logger = LoggerFactory.getLogger(RestartFromKafkaOffsetTest.class);

  @Test
  public void testReadFromKafka() throws Exception {
    // somehow start session
    //stopCurrentSession();
    //startNewSession();

    Config config = ConfigFactory.load();
    // kafka config from main config....
    //startingOffsets = kafkaConfig .getStartingOffsets().replaceAll("TEST_TOPIC", topic);

    logger.info("Now re-starting with starting offsets set to:  {}", startingOffsets);
    // launch test with new session
  }

  protected String getStartingOffsets() {
    return startingOffsets;
  }

  protected ImmutableSet<ImmutableList<Object>> getExpectedResults() {
    return ImmutableSet.of(
      ImmutableList.of(
      )
    );

  }

  public SparkSession startNewSession() {
    session = SparkSession.builder().appName("test").master("local[*]").getOrCreate();  // create default session
    session.sqlContext().setConf("spark.sql.shuffle.partitions", "1");          // cuts a few seconds off execution time
    return session;
  }

  public void stopCurrentSession() {
    session.stop();
  }

}