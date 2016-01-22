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

package com.metamx.tranquility.server.http

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Abort
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Curator
import com.metamx.common.scala.net.curator.Disco
import com.metamx.tranquility.config.ConfigHelper
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.twitter.app.App
import com.twitter.app.Flag
import com.metamx.tranquility.server.ServerConfig
import java.io.FileInputStream
import org.apache.curator.framework.CuratorFramework
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.scala_tools.time.Imports._

object ServerMain extends App with Logging
{
  private val ConfigResource = "tranquility-server.yaml"

  private val configFile: Flag[String] = flag(
    "configFile",
    s"Path to alternate config file, from working directory (default: $ConfigResource from classpath)"
  )

  def main(): Unit = {
    val configInputStream = configFile.get match {
      case Some(file) =>
        log.info(s"Reading configuration from file[$file].")
        new FileInputStream(file)

      case None =>
        log.info(s"Reading configuration from resource[$ConfigResource].")
        getClass.getClassLoader.getResourceAsStream(ConfigResource) mapNull {
          System.err.println(s"Expected resource $ConfigResource (or provide -configFile <path>)")
          sys.exit(1)
        }
    }

    val lifecycle = new Lifecycle
    val (globalConfig, dataSourceConfigs, globalProperties) = ConfigHelper
      .readConfigYaml(configInputStream, classOf[ServerConfig])

    log.info(s"Read configuration for dataSources[${dataSourceConfigs.keys.mkString(", ")}].")

    val servlet = createServlet(lifecycle, dataSourceConfigs)
    val server = createJettyServer(lifecycle, globalConfig, servlet)

    try {
      lifecycle.start()
    }
    catch {
      case e: Exception =>
        Abort(e)
    }

    lifecycle.join()
  }

  def createServlet[T <: TranquilityConfig](
    lifecycle: Lifecycle,
    dataSourceConfigs: Map[String, DataSourceConfig[T]]
  ): TranquilityServlet =
  {
    val curators = ConcurrentMap[String, CuratorFramework]()
    val finagleRegistries = ConcurrentMap[(String, String), FinagleRegistry]()
    val tranquilizers = dataSourceConfigs strictMapValues { dataSourceConfig =>

      // Corral Zookeeper stuff
      val zookeeperConnect = dataSourceConfig.config.zookeeperConnect
      val discoPath = dataSourceConfig.config.discoPath
      val curator = curators.getOrElseUpdate(
        zookeeperConnect,
        Curator.create(zookeeperConnect, dataSourceConfig.config.zookeeperTimeout.standardDuration, lifecycle)
      )
      val finagleRegistry = finagleRegistries.getOrElseUpdate(
        (zookeeperConnect, discoPath), {
          val disco = lifecycle.addManagedInstance(new Disco(curator, dataSourceConfig.config))
          new FinagleRegistry(FinagleRegistryConfig(), disco)
        }
      )

      lifecycle.addManagedInstance(
        ConfigHelper.createTranquilizerScala(
          dataSourceConfig,
          finagleRegistry,
          curator
        )
      )
    }

    new TranquilityServlet(tranquilizers)
  }

  def createJettyServer(
    lifecycle: Lifecycle,
    globalConfig: ServerConfig,
    servlet: TranquilityServlet
  ): Server =
  {
    new Server(new QueuedThreadPool(globalConfig.httpThreads)) withEffect { server =>
      val connector = new ServerConnector(server)
      server.setConnectors(Array(connector))
      connector.setPort(globalConfig.httpPort)
      globalConfig.httpIdleTimeout.standardDuration.millis match {
        case timeout if timeout > 0 =>
          connector.setIdleTimeout(timeout)
        case _ =>
      }

      server.setHandler(
        new ServletHandler withEffect { handler =>
          handler.addServletWithMapping(new ServletHolder(servlet), "/*")
        }
      )

      lifecycle onStart {
        server.start()
      } onStop {
        server.stop()
      }
    }
  }
}
