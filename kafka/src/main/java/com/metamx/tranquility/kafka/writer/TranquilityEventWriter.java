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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.ConfigHelper;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.finagle.FinagleRegistry;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.TranquilityKafkaConfig;
import com.metamx.tranquility.tranquilizer.SimpleTranquilizerAdapter;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import org.apache.curator.framework.CuratorFramework;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Pushes events to Druid through Tranquility using the SimpleTranquilizerAdapter.
 */
public class TranquilityEventWriter
{
  private static final Logger log = new Logger(TranquilityEventWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig;
  private final SimpleTranquilizerAdapter<scala.collection.immutable.Map<String, Object>> simpleTranquilizerAdapter;

  private long rejectedLogCount = 0;

  public TranquilityEventWriter(
      String topic,
      DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig,
      CuratorFramework curator,
      FinagleRegistry finagleRegistry
  )
  {
    this.dataSourceConfig = dataSourceConfig;

    final Tranquilizer<scala.collection.immutable.Map<String, Object>> tranquilizer =
        ConfigHelper.createTranquilizerWithLocation(
            dataSourceConfig,
            finagleRegistry,
            curator,
            DruidLocation.create(
                dataSourceConfig.config().druidIndexingServiceName(),
                dataSourceConfig.config().useTopicAsDataSource()
                ? topic
                : dataSourceConfig.fireDepartment().getDataSchema().getDataSource()
            )
        );

    simpleTranquilizerAdapter = SimpleTranquilizerAdapter.wrap(
        tranquilizer,
        dataSourceConfig.config().reportDropsAsExceptions()
    );

    simpleTranquilizerAdapter.start();
  }

  public void send(byte[] message) throws InterruptedException
  {
    Map<String, Object> map;
    try {
      map = MAPPER.readValue(
          message, new TypeReference<HashMap<String, Object>>()
          {
          }
      );
    }
    catch (IOException e) {
      if (++rejectedLogCount <= 5
          || (rejectedLogCount <= 100 && rejectedLogCount % 10 == 0)
          || rejectedLogCount % 100 == 0) {
        log.debug(e, "%d message(s) failed to parse as JSON and were rejected", rejectedLogCount);
      }
      if (dataSourceConfig.config().reportDropsAsExceptions()) {
        Throwables.propagate(e);
      }

      return;
    }

    // this call may throw an exception for an unrelated message due to SimpleTranquilizerAdapter's implementation
    simpleTranquilizerAdapter.send(
        JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(
            Predef.<Tuple2<String, Object>>conforms()
        )
    );
  }

  public void flush() throws InterruptedException
  {
    simpleTranquilizerAdapter.flush();
  }

  public void stop()
  {
    try {
      simpleTranquilizerAdapter.stop();
    }
    catch (IllegalStateException e) {
      log.info(e, "Exception while stopping Tranquility");
    }
  }

  public MessageCounters getMessageCounters()
  {
    return new MessageCounters(
        simpleTranquilizerAdapter.receivedCount(),
        simpleTranquilizerAdapter.sentCount(),
        simpleTranquilizerAdapter.failedCount()
    );
  }
}
