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

import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.common.scala.net.curator.Disco;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.finagle.FinagleRegistry;
import com.metamx.tranquility.finagle.FinagleRegistryConfig;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.TranquilityKafkaConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Manages the creation and operation of TranquilityEventWriters.
 */
public class WriterController
{
  private static final Logger log = new Logger(WriterController.class);
  private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 500, 30000);

  private List<DataSourceConfig<TranquilityKafkaConfig>> dataSourceConfigList;
  private Map<String, TranquilityEventWriter> writers = new ConcurrentHashMap<>();
  private Map<String, CuratorFramework> curators = new ConcurrentHashMap<>();
  private Map<String, FinagleRegistry> finagleRegistries = new ConcurrentHashMap<>();

  public WriterController(Map<String, DataSourceConfig<TranquilityKafkaConfig>> dataSourceConfigs)
  {
    this.dataSourceConfigList = new ArrayList<>(dataSourceConfigs.values());
    this.dataSourceConfigList.sort(
        new Comparator<DataSourceConfig<TranquilityKafkaConfig>>()
        {
          @Override
          public int compare(DataSourceConfig<TranquilityKafkaConfig> o1, DataSourceConfig<TranquilityKafkaConfig> o2)
          {
            return o2.config().getTopicPatternPriority().compareTo(o1.config().getTopicPatternPriority());
          }
        }
    );

    log.info("Ready: [topicPattern] -> dataSource mappings:");
    for (DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig : this.dataSourceConfigList) {
      log.info(
          "  [%s] -> %s (priority: %d)",
          dataSourceConfig.config().getTopicPattern(),
          dataSourceConfig.dataSource(),
          dataSourceConfig.config().getTopicPatternPriority()
      );
    }
  }

  public synchronized TranquilityEventWriter getWriter(String topic)
  {
    if (!writers.containsKey(topic)) {
      // create a EventWriter using the spec mapped to the first matching topicPattern
      for (DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig : dataSourceConfigList) {
        if (Pattern.matches(dataSourceConfig.config().getTopicPattern(), topic)) {
          log.info(
              "Creating EventWriter for topic [%s] using dataSource [%s]",
              topic,
              dataSourceConfig.dataSource()
          );
          writers.put(topic, createWriter(topic, dataSourceConfig));
          return writers.get(topic);
        }
      }

      throw new RuntimeException(String.format("Kafka topicFilter allowed topic [%s] but no spec is mapped", topic));
    }

    return writers.get(topic);
  }

  public Map<String, MessageCounters> flushAll() throws InterruptedException
  {
    Map<String, MessageCounters> flushedEvents = new HashMap<>();
    for (Map.Entry<String, TranquilityEventWriter> entry : writers.entrySet()) {
      entry.getValue().flush();
      flushedEvents.put(entry.getKey(), entry.getValue().getMessageCounters());
    }

    return flushedEvents;
  }

  public void stop()
  {
    for (Map.Entry<String, TranquilityEventWriter> entry : writers.entrySet()) {
      entry.getValue().stop();
    }

    for (Map.Entry<String, CuratorFramework> entry : curators.entrySet()) {
      entry.getValue().close();
    }
  }

  protected TranquilityEventWriter createWriter(String topic, DataSourceConfig<TranquilityKafkaConfig> dataSourceConfig)
  {
    final String curatorKey = dataSourceConfig.config().zookeeperConnect();
    if (!curators.containsKey(curatorKey)) {
      final int zkTimeout = Ints.checkedCast(
          dataSourceConfig.config()
                          .zookeeperTimeout()
                          .toStandardDuration()
                          .getMillis()
      );

      final CuratorFramework curator = CuratorFrameworkFactory.builder()
                                                              .connectString(
                                                                  dataSourceConfig.config()
                                                                                  .zookeeperConnect()
                                                              )
                                                              .connectionTimeoutMs(zkTimeout)
                                                              .retryPolicy(RETRY_POLICY)
                                                              .build();
      curator.start();
      curators.put(curatorKey, curator);
    }

    final String finagleKey = String.format("%s:%s", curatorKey, dataSourceConfig.config().discoPath());
    if (!finagleRegistries.containsKey(finagleKey)) {
      finagleRegistries.put(
          finagleKey, new FinagleRegistry(
              FinagleRegistryConfig.builder().build(),
              new Disco(curators.get(curatorKey), dataSourceConfig.config())
          )
      );
    }

    return new TranquilityEventWriter(
        topic,
        dataSourceConfig,
        curators.get(curatorKey),
        finagleRegistries.get(finagleKey)
    );
  }
}
