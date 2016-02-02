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

package com.metamx.tranquility.example

import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.Return
import com.twitter.util.Throw
import org.joda.time.DateTime
import scala.collection.JavaConverters._

object ScalaExample extends Logging
{
  def main(args: Array[String]) {
    // Read config from "example.json" on the classpath.
    val configStream = getClass.getClassLoader.getResourceAsStream("example.json")
    val config: TranquilityConfig[PropertiesBasedConfig] = TranquilityConfig.read(configStream)
    val wikipediaConfig: DataSourceConfig[PropertiesBasedConfig] = config.getDataSource("wikipedia")
    val sender: Tranquilizer[java.util.Map[String, AnyRef]] = DruidBeams
      .fromConfig(config.getDataSource("wikipedia"))
      .buildTranquilizer(wikipediaConfig.tranquilizerBuilder())

    sender.start()

    try {
      // Send 10000 objects.

      for (i <- 0 until 10000) {
        val obj = Map[String, AnyRef](
          "timestamp" -> new DateTime().toString,
          "page" -> "foo",
          "added" -> Int.box(i)
        )

        // Asynchronously send event to Druid:
        sender.send(obj.asJava) respond {
          case Return(_) =>
            log.info("Sent message: %s", obj)

          case Throw(e: MessageDroppedException) =>
            log.warn(e, "Dropped message: %s", obj)

          case Throw(e) =>
            log.error(e, "Failed to send message: %s", obj)
        }
      }
    }
    finally {
      sender.flush()
      sender.stop()
    }
  }
}
