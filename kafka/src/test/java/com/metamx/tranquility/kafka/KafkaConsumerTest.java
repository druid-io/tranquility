/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.metamx.common.scala.Jackson$;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.druid.DruidGuicer$;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.PropertiesBasedKafkaConfig;
import com.metamx.tranquility.kafka.writer.TranquilityEventWriter;
import com.metamx.tranquility.kafka.writer.WriterController;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import junit.framework.Assert;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.network.BlockingChannel;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skife.config.ConfigurationObjectFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerTest
{
  private static final String GROUP_ID = "test";
  private static final String CLIENT_ID = "test";
  private static final String MESSAGE = "message";

  private static File tempDir;
  private static TestingServer zk;
  private static KafkaServerStartable kafka;
  private static KafkaProducer<String, String> producer;
  private static BlockingChannel channel;
  private static Properties consumerProperties;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
    tempDir = Files.createTempDir();
    zk = new TestingServer();
    kafka = new KafkaServerStartable(getKafkaTestConfig());
    kafka.startup();

    Properties props = new Properties();
    props.put(
        "bootstrap.servers",
        String.format("%s:%s", kafka.serverConfig().hostName(), kafka.serverConfig().port())
    );
    producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

    channel = new BlockingChannel(
        kafka.serverConfig().hostName(),
        kafka.serverConfig().port(),
        BlockingChannel.UseDefaultBufferSize(),
        BlockingChannel.UseDefaultBufferSize(),
        5000
    );
    channel.connect();

    consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", GROUP_ID);
    consumerProperties.setProperty("zookeeper.connect", zk.getConnectString());
    consumerProperties.setProperty("kafka.zookeeper.connect", zk.getConnectString());
    consumerProperties.setProperty("commit.periodMillis", "90000");
    consumerProperties.setProperty("auto.offset.reset", "smallest");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception
  {
    channel.disconnect();
    producer.close();
    kafka.shutdown();
    zk.close();
    FileUtils.deleteDirectory(tempDir);
  }

  private static KafkaConfig getKafkaTestConfig() throws Exception
  {
    int port;
    try (ServerSocket server = new ServerSocket(0)) {
      port = server.getLocalPort();
    }

    Properties props = new Properties();
    props.put("broker.id", "0");
    props.put("host.name", "localhost");
    props.put("port", String.valueOf(port));
    props.put("log.dir", tempDir.getPath());
    props.put("zookeeper.connect", zk.getConnectString());
    props.put("replica.socket.timeout.ms", "1500");
    return new KafkaConfig(props);
  }

  @Test(timeout = 60_000L)
  public void testStartConsumersNoCommit() throws Exception
  {
    final String topic = "testStartConsumersNoCommit";
    final int numMessages = 5;
    final CountDownLatch latch = new CountDownLatch(numMessages);

    consumerProperties.setProperty("topicPattern", topic);

    TranquilityEventWriter mockEventWriter = EasyMock.mock(TranquilityEventWriter.class);
    mockEventWriter.send((byte[]) EasyMock.anyObject());
    EasyMock.expectLastCall().andStubAnswer(
        new IAnswer<Void>()
        {
          @Override
          public Void answer() throws Throwable
          {
            latch.countDown();
            return null;
          }
        }
    );

    WriterController mockWriterController = EasyMock.mock(WriterController.class);
    EasyMock.expect(mockWriterController.getWriter(topic)).andReturn(mockEventWriter).times(numMessages);
    EasyMock.expect(mockWriterController.flushAll()).andReturn(Maps.<String, MessageCounters>newHashMap());

    mockWriterController.stop();
    EasyMock.expectLastCall();

    EasyMock.replay(mockWriterController, mockEventWriter);

    PropertiesBasedKafkaConfig config = new ConfigurationObjectFactory(consumerProperties).build(
        PropertiesBasedKafkaConfig.class);

    FireDepartment fd = new FireDepartment(
        new DataSchema(topic, null, new AggregatorFactory[]{}, null, new ObjectMapper()),
        new RealtimeIOConfig(
            new LocalFirehoseFactory(null, null, null), new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics)
          {
            return null;
          }
        }, null
        ),
        null
    );

    Map<String, DataSourceConfig<PropertiesBasedKafkaConfig>> datasourceConfigs = ImmutableMap.of(
        topic,
        new DataSourceConfig<>(
            topic,
            config,
            fireDepartmentToScalaMap(fd)
        )
    );

    // commitMillis is set high enough that the commit thread should not run during the test
    KafkaConsumer kafkaConsumer = new KafkaConsumer(
        config,
        consumerProperties,
        datasourceConfigs,
        mockWriterController
    );
    kafkaConsumer.start();

    Assert.assertEquals("Unexpected consumer offset", -1, getConsumerOffset(topic));

    for (int i = numMessages; i > 0; i--) {
      producer.send(new ProducerRecord<String, String>(topic, MESSAGE)).get();
    }
    latch.await();

    // check that offset wasn't committed since commit thread didn't run
    Assert.assertEquals("Unexpected consumer offset", -1, getConsumerOffset(topic));

    kafkaConsumer.stop();

    // check that offset was committed on shutdown
    Assert.assertEquals("Unexpected consumer offset", numMessages, getConsumerOffset(topic));
    EasyMock.verify(mockWriterController, mockEventWriter);
  }

  @Test(timeout = 60_000L)
  public void testStartConsumersWithCommitThread() throws Exception
  {
    final String topic = "testStartConsumersWithCommitThread";
    final int numMessages = 8;
    final CountDownLatch latch = new CountDownLatch(numMessages);

    consumerProperties.setProperty("topicPattern", topic);

    TranquilityEventWriter mockEventWriter = EasyMock.mock(TranquilityEventWriter.class);
    mockEventWriter.send((byte[]) EasyMock.anyObject());
    EasyMock.expectLastCall().andStubAnswer(
        new IAnswer<Void>()
        {
          @Override
          public Void answer() throws Throwable
          {
            latch.countDown();
            return null;
          }
        }
    );

    WriterController mockWriterController = EasyMock.mock(WriterController.class);
    EasyMock.expect(mockWriterController.getWriter(topic)).andReturn(mockEventWriter).times(numMessages);
    EasyMock.expect(mockWriterController.flushAll()).andReturn(Maps.<String, MessageCounters>newHashMap()).times(2);

    mockWriterController.stop();
    EasyMock.expectLastCall();

    EasyMock.replay(mockWriterController, mockEventWriter);

    PropertiesBasedKafkaConfig config = new ConfigurationObjectFactory(consumerProperties).build(
        PropertiesBasedKafkaConfig.class);

    FireDepartment fd = new FireDepartment(
        new DataSchema(topic, null, new AggregatorFactory[]{}, null, new ObjectMapper()),
        new RealtimeIOConfig(
            new LocalFirehoseFactory(null, null, null), new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics)
          {
            return null;
          }
        }, null
        ),
        null
    );

    Map<String, DataSourceConfig<PropertiesBasedKafkaConfig>> datasourceConfigs = ImmutableMap.of(
        topic,
        new DataSourceConfig<>(
            topic,
            config,
            fireDepartmentToScalaMap(fd)
        )
    );

    // commitMillis is set high enough that the commit thread should not run during the test
    KafkaConsumer kafkaConsumer = new KafkaConsumer(
        config,
        consumerProperties,
        datasourceConfigs,
        mockWriterController
    );
    kafkaConsumer.start();

    Assert.assertEquals("Unexpected consumer offset", -1, getConsumerOffset(topic));

    for (int i = numMessages; i > 0; i--) {
      producer.send(new ProducerRecord<String, String>(topic, MESSAGE)).get();
    }
    latch.await();

    kafkaConsumer.commit();

    // check that offset was committed since commit ran
    Assert.assertEquals("Unexpected consumer offset", numMessages, getConsumerOffset(topic));

    kafkaConsumer.stop();

    Assert.assertEquals("Unexpected consumer offset", numMessages, getConsumerOffset(topic));
    EasyMock.verify(mockWriterController, mockEventWriter);
  }

  private long getConsumerOffset(String topic)
  {
    TopicAndPartition partition = new TopicAndPartition(topic, 0);
    OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
        GROUP_ID, Lists.newArrayList(partition),
        (short) 0, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        0, CLIENT_ID
    );

    if (!channel.isConnected()) {
      channel.connect();
    }

    channel.send(fetchRequest.underlying());

    OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());
    OffsetMetadataAndError result = fetchResponse.offsets().get(partition);
    return result.offset();
  }

  public static scala.collection.immutable.Map<String, Object> fireDepartmentToScalaMap(
      final FireDepartment fireDepartment
  ) throws IOException
  {
    return Jackson$.MODULE$.newObjectMapper().readValue(
        DruidGuicer$.MODULE$.Default().objectMapper().writeValueAsBytes(fireDepartment),
        scala.collection.immutable.Map.class
    );
  }
}
