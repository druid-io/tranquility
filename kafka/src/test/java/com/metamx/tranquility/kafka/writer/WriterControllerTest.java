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

package com.metamx.tranquility.kafka.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.TranquilityKafkaConfig;
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
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.skife.config.ConfigurationObjectFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WriterControllerTest
{
  class TestableWriterController extends WriterController
  {

    private Map<String, TranquilityEventWriter> mockWriters = new HashMap<>();

    public TestableWriterController(Map<String, DataSourceConfig<TranquilityKafkaConfig>> dataSourceConfigs)
    {
      super(dataSourceConfigs);
    }

    protected TranquilityEventWriter createWriter(
        String topic,
        DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig
    )
    {
      TranquilityEventWriter writer = EasyMock.mock(TranquilityEventWriter.class);
      mockWriters.put(topic, writer);
      return writer;
    }
  }

  private TestableWriterController writerController;

  @Before
  public void setUp()
  {
    Properties props = new Properties();
    props.setProperty("kafka.zookeeper.connect", "localhost:2181");
    props.setProperty("zookeeper.connect", "localhost:2181");

    Properties propsTwitter = new Properties(props);
    propsTwitter.setProperty("topicPattern", "twitter");

    Properties propsTest = new Properties(props);
    propsTest.setProperty("topicPattern", "test[0-9]");
    propsTest.setProperty("useTopicAsDataSource", "true");

    FireDepartment fd = new FireDepartment(
        new DataSchema("twitter", null, new AggregatorFactory[]{}, null, new ObjectMapper()),
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

    Map<String, DataSourceConfig<TranquilityKafkaConfig>> datasourceConfigs = ImmutableMap.of(
        "twitter",
        new DataSourceConfig<>(new ConfigurationObjectFactory(propsTwitter).build(TranquilityKafkaConfig.class), fd),
        "test[0-9]",
        new DataSourceConfig<>(new ConfigurationObjectFactory(propsTest).build(TranquilityKafkaConfig.class), fd)
    );

    writerController = new TestableWriterController(datasourceConfigs);
  }

  @Test
  public void testGetWriter()
  {
    Assert.assertEquals(0, writerController.mockWriters.size());

    TranquilityEventWriter writerTwitter = writerController.getWriter("twitter");

    Assert.assertEquals(1, writerController.mockWriters.size());
    Assert.assertSame(writerTwitter, writerController.mockWriters.get("twitter"));
    Assert.assertSame(writerTwitter, writerController.getWriter("twitter"));

    TranquilityEventWriter writerTest0 = writerController.getWriter("test0");
    TranquilityEventWriter writerTest1 = writerController.getWriter("test1");

    Assert.assertEquals(3, writerController.mockWriters.size());
    Assert.assertSame(writerTest0, writerController.getWriter("test0"));
    Assert.assertSame(writerTest1, writerController.getWriter("test1"));
  }

  @Test(expected = RuntimeException.class)
  public void testGetWriterInvalid()
  {
    writerController.getWriter("test10");
  }

  @Test
  public void testFlushAll() throws InterruptedException
  {
    writerController.getWriter("test0");
    writerController.getWriter("test1");
    TranquilityEventWriter mock0 = writerController.mockWriters.get("test0");
    TranquilityEventWriter mock1 = writerController.mockWriters.get("test1");

    mock0.flush();
    mock1.flush();
    EasyMock.expect(mock0.getMessageCounters()).andReturn(new MessageCounters(1, 2, 3));
    EasyMock.expect(mock1.getMessageCounters()).andReturn(new MessageCounters(4, 5, 6));
    EasyMock.replay(mock0, mock1);

    Map<String, MessageCounters> results = writerController.flushAll();

    EasyMock.verify(mock0, mock1);

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, results.get("test0").getReceivedCount());
    Assert.assertEquals(2, results.get("test0").getSentCount());
    Assert.assertEquals(3, results.get("test0").getFailedCount());
    Assert.assertEquals(4, results.get("test1").getReceivedCount());
    Assert.assertEquals(5, results.get("test1").getSentCount());
    Assert.assertEquals(6, results.get("test1").getFailedCount());
  }

  @Test
  public void testStop()
  {
    writerController.getWriter("test0");
    writerController.getWriter("test1");
    TranquilityEventWriter mock0 = writerController.mockWriters.get("test0");
    TranquilityEventWriter mock1 = writerController.mockWriters.get("test1");

    mock0.stop();
    mock1.stop();
    EasyMock.replay(mock0, mock1);

    writerController.stop();

    EasyMock.verify(mock0, mock1);
  }
}
