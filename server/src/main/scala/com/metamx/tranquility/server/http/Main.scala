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
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Curator
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidGuicer
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.server.config.DataSourceConfig
import com.metamx.tranquility.server.config.GeneralConfig
import com.metamx.tranquility.server.config.GlobalConfig
import com.metamx.tranquility.server.config.GlobalProperties
import com.metamx.tranquility.tranquilizer.Tranquilizer
import io.druid.segment.realtime.FireDepartment
import java.io.FileInputStream
import java.io.InputStream
import java.util.Properties
import org.apache.curator.framework.CuratorFramework
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.scala_tools.time.Imports._
import org.skife.config.ConfigurationObjectFactory

object Main
{
  private val ConfigResource = "tranquility-server.yaml"

  def main(args: Array[String]): Unit = {
    val configInputStream = if (args.isEmpty) {
      getClass.getClassLoader.getResourceAsStream(ConfigResource) mapNull {
        System.err.println(s"Expected resource $ConfigResource (or provide path on the command line)")
        sys.exit(1)
      }
    } else {
      new FileInputStream(args(0))
    }

    val lifecycle = new Lifecycle
    val (globalConfig, dataSourceConfigs) = readConfigYaml(configInputStream)
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

  def readConfigYaml(in: InputStream): (GlobalConfig, Map[String, DataSourceConfig]) = {
    val yaml = in.withFinally(_.close()) { in =>
      dict(Yaml.load(in))
    }
    val globalProperties: Dict = getAsDict(yaml, "properties")
    val globalConfig = mapConfig(globalProperties, classOf[GlobalConfig])

    val dataSources: Dict = getAsDict(yaml, "dataSources")
    val dataSourceConfigs = for ((dataSource, x) <- dataSources) yield {
      val d = dict(x)
      val dataSourceProperties = getAsDict(d, "properties")
      for (k <- GlobalProperties) {
        require(!dataSourceProperties.contains(k), s"Property '$k' cannot be per-dataSource (found in[$dataSource]).")
      }

      val dataSourceSpec = getAsDict(d, "spec")
      val fireDepartment = DruidGuicer.objectMapper.convertValue(normalizeJava(dataSourceSpec), classOf[FireDepartment])
      val config = mapConfig(globalProperties ++ dataSourceProperties, classOf[GeneralConfig])

      // Sanity check: two ways of providing dataSource, they must match
      require(
        dataSource == fireDepartment.getDataSchema.getDataSource,
        s"dataSource[$dataSource] did not match spec[${fireDepartment.getDataSchema.getDataSource}]"
      )

      (dataSource, DataSourceConfig(config, fireDepartment))
    }

    (globalConfig, dataSourceConfigs)
  }

  def mapConfig[A](d: Dict, clazz: Class[A]): A = {
    val properties = new Properties
    for ((k, v) <- d if v != null) {
      properties.setProperty(k, String.valueOf(v))
    }
    val configFactory = new ConfigurationObjectFactory(properties)
    configFactory.build(clazz)
  }

  def createServlet(
    lifecycle: Lifecycle,
    dataSourceConfigs: Map[String, DataSourceConfig]
  ): TranquilityServlet =
  {
    val curators = ConcurrentMap[String, CuratorFramework]()
    val finagleRegistries = ConcurrentMap[(String, String), FinagleRegistry]()
    val tranquilizers = dataSourceConfigs strictMapValues { dataSourceConfig =>
      val environment = DruidEnvironment(dataSourceConfig.config.druidIndexingServiceName)

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

      // Create beam for this dataSource
      val beam = DruidBeams
        .builder(environment, dataSourceConfig.fireDepartment)
        .curator(curator)
        .finagleRegistry(finagleRegistry)
        .partitions(dataSourceConfig.config.taskPartitions)
        .replicants(dataSourceConfig.config.taskReplicants)
        .druidBeamConfig(dataSourceConfig.config.druidBeamConfig)
        .buildBeam()

      lifecycle.addManagedInstance(
        Tranquilizer(
          beam,
          dataSourceConfig.config.tranquilityMaxBatchSize,
          dataSourceConfig.config.tranquilityMaxPendingBatches,
          dataSourceConfig.config.tranquilityLingerMillis
        )
      )
    }

    new TranquilityServlet(tranquilizers)
  }

  def createJettyServer(
    lifecycle: Lifecycle,
    globalConfig: GlobalConfig,
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

  private def getAsDict(d: Dict, k: String): Dict = {
    Option(d.getOrElse(k, null)).map(dict(_)).getOrElse(Dict())
  }

  private class ZookeeperStuff(
    val curator: CuratorFramework,
    val disco: Disco,
    val finagleRegistry: FinagleRegistry
  )

}
