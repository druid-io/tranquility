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

package com.metamx.tranquility.example;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.joda.time.DateTime;
import scala.runtime.BoxedUnit;

import java.io.InputStream;
import java.util.Map;

public class JavaExample
{
  private static final Logger log = new Logger(JavaExample.class);

  public static void main(String[] args)
  {
    // Read config from "example.json" on the classpath.
    final InputStream configStream = JavaExample.class.getClassLoader().getResourceAsStream("example.json");
    final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
    final DataSourceConfig<PropertiesBasedConfig> wikipediaConfig = config.getDataSource("wikipedia");
    final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
                                                               .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());

    sender.start();

    try {
      // Send 10000 objects

      for (int i = 0; i < 10000; i++) {
        // Build a sample event to send; make sure we use a current date
        final Map<String, Object> obj = ImmutableMap.<String, Object>of(
            "timestamp", new DateTime().toString(),
            "page", "foo",
            "added", i
        );

        // Asynchronously send event to Druid:
        sender.send(obj).addEventListener(
            new FutureEventListener<BoxedUnit>()
            {
              @Override
              public void onSuccess(BoxedUnit value)
              {
                log.info("Sent message: %s", obj);
              }

              @Override
              public void onFailure(Throwable e)
              {
                if (e instanceof MessageDroppedException) {
                  log.warn(e, "Dropped message: %s", obj);
                } else {
                  log.error(e, "Failed to send message: %s", obj);
                }
              }
            }
        );
      }
    }
    finally {
      sender.flush();
      sender.stop();
    }
  }
}
