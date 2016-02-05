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

package com.metamx.tranquility.console

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.app.App
import com.twitter.app.Flag
import com.twitter.util.Return
import com.twitter.util.Throw
import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

object ConsoleMain extends App with Logging
{
  private val configFileFlag: Flag[String] = flag(
    "configFile",
    "Path to config file, relative to working directory"
  )

  private val dataSourceFlag: Flag[String] = flag(
    "dataSource",
    "dataSource to send to (default = only one in the file)"
  )

  def main(): Unit = {
    val config: TranquilityConfig[PropertiesBasedConfig] = configFileFlag.get match {
      case Some(file) =>
        log.info(s"Reading configuration from file[$file].")
        TranquilityConfig.read(new FileInputStream(file))

      case None =>
        System.err.println(s"Expected -configFile <path>")
        sys.exit(1)
    }

    val dataSourceConfig: DataSourceConfig[PropertiesBasedConfig] = dataSourceFlag.get match {
      case Some(dataSource) =>
        config.dataSourceConfigs.getOrElse(
          dataSource, {
            throw new IllegalArgumentException(
              s"dataSource[$dataSource] not found in configFile[${configFileFlag.get}]."
            )
          }
        )

      case None if config.dataSourceConfigs.size == 1 =>
        config.dataSourceConfigs.head._2

      case None =>
        throw new IllegalArgumentException(
          s"Multiple dataSources found in configFile[${configFileFlag.get}], please specify one with -dataSource <name>."
        )
    }

    log.info(s"Sending data to dataSource[${dataSourceConfig.dataSource}].")

    val sender: Tranquilizer[java.util.Map[String, AnyRef]] = DruidBeams
      .fromConfig(dataSourceConfig)
      .buildTranquilizer(dataSourceConfig.tranquilizerBuilder())

    val sent = new AtomicLong
    val dropped = new AtomicLong
    val failed = new AtomicLong
    val exception = new AtomicReference[Throwable]()
    def maybeThrow(): Unit = {
      if (exception.get() != null) {
        throw exception.get()
      }
    }

    sender.start()

    try {
      val objectMapper = new ObjectMapper()
      val iterator: MappingIterator[java.util.Map[String, AnyRef]] = objectMapper.readValues(
        objectMapper.getFactory.createParser(System.in),
        new TypeReference[java.util.Map[String, AnyRef]]()
        {}
      )

      try {
        while (iterator.hasNext) {
          val obj = iterator.next()

          maybeThrow()

          sender.send(obj) respond {
            case Return(_) =>
              sent.incrementAndGet()
              log.debug("Sent message: %s", obj)

            case Throw(e: MessageDroppedException) =>
              dropped.incrementAndGet()
              log.warn("Dropped message: %s", obj)

            case Throw(e) =>
              failed.incrementAndGet()
              exception.compareAndSet(null, e)
              log.error(e, "Failed to send message: %s", obj)
          }
        }
      }
      finally {
        iterator.close()
      }
    }
    finally {
      sender.flush()
      sender.stop()
    }

    maybeThrow()

    log.info(s"Sent[${sent.get()}], dropped[${dropped.get()}], failed[${failed.get()}] messages to Druid.")
  }
}
