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
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.finagle.FinagleRegistry;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.PropertiesBasedKafkaConfig;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.apache.curator.framework.CuratorFramework;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pushes events to Druid through Tranquility using the SimpleTranquilizerAdapter.
 */
public class TranquilityEventWriter
{
  private static final Logger log = new Logger(TranquilityEventWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final DataSourceConfig<PropertiesBasedKafkaConfig> dataSourceConfig;
  private final Tranquilizer<Map<String, Object>> tranquilizer;

  private final AtomicLong receivedCounter = new AtomicLong();
  private final AtomicLong sentCounter = new AtomicLong();
  private final AtomicLong failedCounter = new AtomicLong();
  private final AtomicLong rejectedLogCounter = new AtomicLong();
  private final AtomicReference<Throwable> exception = new AtomicReference<>();

  public TranquilityEventWriter(
      String topic,
      DataSourceConfig<PropertiesBasedKafkaConfig> dataSourceConfig,
      CuratorFramework curator,
      FinagleRegistry finagleRegistry
  )
  {
    this.dataSourceConfig = dataSourceConfig;
    this.tranquilizer =
        DruidBeams.fromConfig(dataSourceConfig)
                  .location(DruidLocation.create(
                      dataSourceConfig.propertiesBasedConfig().druidIndexingServiceName(),
                      dataSourceConfig.propertiesBasedConfig().useTopicAsDataSource()
                      ? topic
                      : dataSourceConfig.dataSource()
                  ))
                  .curator(curator)
                  .finagleRegistry(finagleRegistry)
                  .buildTranquilizer(dataSourceConfig.tranquilizerBuilder());
    this.tranquilizer.start();
  }

  public void send(byte[] message) throws InterruptedException
  {
    receivedCounter.incrementAndGet();

    Map<String, Object> map;
    try {
      map = MAPPER.readValue(
          message, new TypeReference<HashMap<String, Object>>()
          {
          }
      );
    }
    catch (IOException e) {
      failedCounter.incrementAndGet();

      final long rejectedLogCount = rejectedLogCounter.incrementAndGet();
      if (rejectedLogCount <= 5
          || (rejectedLogCount <= 100 && rejectedLogCount % 10 == 0)
          || rejectedLogCount % 100 == 0) {
        log.debug(e, "%d message(s) failed to parse as JSON and were rejected", rejectedLogCount);
      }

      if (dataSourceConfig.propertiesBasedConfig().reportDropsAsExceptions()) {
        throw Throwables.propagate(e);
      }

      return;
    }

    tranquilizer.send(map).addEventListener(
        new FutureEventListener<BoxedUnit>()
        {
          @Override
          public void onSuccess(BoxedUnit value)
          {
            sentCounter.incrementAndGet();
          }

          @Override
          public void onFailure(Throwable cause)
          {
            failedCounter.incrementAndGet();

            if (!dataSourceConfig.propertiesBasedConfig().reportDropsAsExceptions()
                && cause instanceof MessageDroppedException) {
              return;
            }

            exception.compareAndSet(null, cause);
          }
        }
    );

    maybeThrow();
  }

  public void flush() throws InterruptedException
  {
    tranquilizer.flush();
    maybeThrow();
  }

  public void stop()
  {
    try {
      tranquilizer.stop();
    }
    catch (IllegalStateException e) {
      log.info(e, "Exception while stopping Tranquility");
    }
  }

  public MessageCounters getMessageCounters()
  {
    return new MessageCounters(
        receivedCounter.get(),
        sentCounter.get(),
        failedCounter.get()
    );
  }

  private void maybeThrow()
  {
    if (exception.get() != null) {
      throw Throwables.propagate(exception.get());
    }
  }
}
