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
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.finagle.FinagleRegistry;
import com.metamx.tranquility.kafka.KafkaBeamUtils;
import com.metamx.tranquility.kafka.model.MessageCounters;
import com.metamx.tranquility.kafka.model.PropertiesBasedKafkaConfig;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.apache.curator.framework.CuratorFramework;
import scala.runtime.BoxedUnit;

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
  private final Tranquilizer<byte[]> tranquilizer;

  private final AtomicLong receivedCounter = new AtomicLong();
  private final AtomicLong sentCounter = new AtomicLong();
  private final AtomicLong droppedCounter = new AtomicLong();
  private final AtomicLong unparseableCounter = new AtomicLong();
  private final AtomicReference<Throwable> exception = new AtomicReference<>();

  public TranquilityEventWriter(
      String topic,
      DataSourceConfig<PropertiesBasedKafkaConfig> dataSourceConfig,
      CuratorFramework curator,
      FinagleRegistry finagleRegistry
  )
  {
    this.dataSourceConfig = dataSourceConfig;
    this.tranquilizer = KafkaBeamUtils.createTranquilizer(
        topic,
        dataSourceConfig,
        curator,
        finagleRegistry
    );
    this.tranquilizer.start();
  }

  public void send(byte[] message) throws InterruptedException
  {
    receivedCounter.incrementAndGet();
    tranquilizer.send(message).addEventListener(
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
            if (cause instanceof MessageDroppedException) {
              droppedCounter.incrementAndGet();
              if (!dataSourceConfig.propertiesBasedConfig().reportDropsAsExceptions()) {
                return;
              }
            } else if (cause instanceof ParseException) {
              unparseableCounter.incrementAndGet();
              if (!dataSourceConfig.propertiesBasedConfig().reportParseExceptions()) {
                return;
              }
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
        droppedCounter.get(),
        unparseableCounter.get()
    );
  }

  private void maybeThrow()
  {
    final Throwable e = exception.get();
    if (e != null) {
      throw Throwables.propagate(e);
    }
  }
}
