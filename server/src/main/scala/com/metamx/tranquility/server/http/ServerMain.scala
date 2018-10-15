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

import java.util

import com.github.nscala_time.time.Imports._
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.net.curator.Curator
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.Abort
import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.security.SSLContextMaker
import com.metamx.tranquility.server.PropertiesBasedServerConfig
import com.twitter.app.App
import com.twitter.app.Flag
import io.druid.data.input.InputRow
import java.io.FileInputStream
import org.apache.curator.framework.CuratorFramework
import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.ServletHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.QueuedThreadPool
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.typeTag

object ServerMain extends App with Logging
{
  private val HTTP_1_1_STRING: String = "HTTP/1.1"
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
    val config = TranquilityConfig.read(configInputStream, classOf[PropertiesBasedServerConfig])

    log.info(s"Read configuration for dataSources[${config.dataSourceConfigs.keys.mkString(", ")}].")

    val servlet = createServlet(lifecycle, config)
    val server = createJettyServer(lifecycle, config, servlet)

    try {
      lifecycle.start()
    }
    catch {
      case e: Exception =>
        Abort(e)
    }

    lifecycle.join()
  }

  def createServlet(
    lifecycle: Lifecycle,
    config: TranquilityConfig[PropertiesBasedServerConfig]
  ): TranquilityServlet =
  {
    val curators = ConcurrentMap[String, CuratorFramework]()
    val finagleRegistries = ConcurrentMap[(String, String), FinagleRegistry]()
    val bundles = config.dataSourceConfigs strictMapValues { dataSourceConfig =>
      // Corral Zookeeper stuff
      val zookeeperConnect = dataSourceConfig.propertiesBasedConfig.zookeeperConnect
      val discoPath = dataSourceConfig.propertiesBasedConfig.discoPath
      val curator = curators.getOrElseUpdate(
        zookeeperConnect,
        Curator.create(
          zookeeperConnect,
          dataSourceConfig.propertiesBasedConfig.zookeeperTimeout.standardDuration,
          lifecycle
        )
      )
      val finagleRegistry = finagleRegistries.getOrElseUpdate(
        (zookeeperConnect, discoPath), {
          val disco = lifecycle.addManagedInstance(new Disco(curator, dataSourceConfig.propertiesBasedConfig))
          val sslContext = SSLContextMaker.createSSLContextOption(
            Some(config.globalConfig.tlsEnable),
            Some(config.globalConfig.tlsProtocol),
            Some(config.globalConfig.tlsTrustStoreType),
            Some(config.globalConfig.tlsTrustStorePath),
            Some(config.globalConfig.tlsTrustStoreAlgorithm),
            Some(config.globalConfig.tlsTrustStorePassword)
          )
          new FinagleRegistry(FinagleRegistryConfig(sslContextOption = sslContext), disco)
        }
      )

      val tranquilizer = lifecycle.addManagedInstance(
        DruidBeams.fromConfig(dataSourceConfig, typeTag[InputRow])
          .curator(curator)
          .finagleRegistry(finagleRegistry)
          .buildTranquilizer(dataSourceConfig.tranquilizerBuilder())
      )
      val parseSpec = DruidBeams.makeFireDepartment(dataSourceConfig).getDataSchema.getParser.getParseSpec
      new DataSourceBundle(tranquilizer, parseSpec)
    }

    new TranquilityServlet(bundles)
  }

  def createPlaintextConnector(
    server: Server,
    config: TranquilityConfig[PropertiesBasedServerConfig]
  ): ServerConnector =
  {
    val connector = new ServerConnector(server)
    connector.setPort(config.globalConfig.httpPort)
    config.globalConfig.httpIdleTimeout.standardDuration.millis match {
      case timeout if timeout > 0 =>
        connector.setIdleTimeout(timeout)
      case _ =>
    }
    connector
  }

  def createTLSConnector(
    server: Server,
    config: TranquilityConfig[PropertiesBasedServerConfig]
  ): ServerConnector =
  {
    val sslContextFactory = createSslContextFactory(config)

    val httpsConfiguration : HttpConfiguration = new HttpConfiguration
    httpsConfiguration.setSecureScheme("https")
    httpsConfiguration.setSecurePort(config.globalConfig.httpsPort)
    httpsConfiguration.addCustomizer(new SecureRequestCustomizer)
    httpsConfiguration.setRequestHeaderSize(8 * 1024)

    val connector : ServerConnector = new ServerConnector(
      server,
      new SslConnectionFactory(sslContextFactory, HTTP_1_1_STRING),
      new HttpConnectionFactory(httpsConfiguration)
    )

    connector.setPort(config.globalConfig.httpsPort)
    config.globalConfig.httpIdleTimeout.standardDuration.millis match {
      case timeout if timeout > 0 =>
        connector.setIdleTimeout(timeout)
      case _ =>
    }
    connector
  }

  def createSslContextFactory(
    config: TranquilityConfig[PropertiesBasedServerConfig]
  ): SslContextFactory = {
    val sslContextFactory = new SslContextFactory(false)
    sslContextFactory.setKeyStorePath(config.globalConfig.httpsKeyStorePath)
    sslContextFactory.setKeyStoreType(config.globalConfig.httpsKeyStoreType)
    sslContextFactory.setKeyStorePassword(config.globalConfig.httpsKeyStorePassword)
    sslContextFactory.setCertAlias(config.globalConfig.httpsCertAlias)
    sslContextFactory.setSslKeyManagerFactoryAlgorithm(config.globalConfig.httpsKeyManagerFactoryAlgorithm)
    sslContextFactory.setKeyManagerPassword(config.globalConfig.httpsKeyManagerPassword)
    if (config.globalConfig.httpsIncludeCipherSuites != null) {
      val suites: Array[String] = config.globalConfig.httpsIncludeCipherSuites.toArray(
        new Array[String](config.globalConfig.httpsIncludeCipherSuites.size())
      )
      sslContextFactory.setIncludeCipherSuites(suites: _*)
    }
    if (config.globalConfig.httpsExcludeCipherSuites != null) {
      val suites: Array[String] = config.globalConfig.httpsExcludeCipherSuites.toArray(
        new Array[String](config.globalConfig.httpsExcludeCipherSuites.size())
      )
      sslContextFactory.setExcludeCipherSuites(suites: _*)
    }
    if (config.globalConfig.httpsIncludeProtocols != null) {
      val protocols: Array[String] = config.globalConfig.httpsIncludeProtocols.toArray(
        new Array[String](config.globalConfig.httpsIncludeProtocols.size())
      )
      sslContextFactory.setIncludeProtocols(protocols: _*)
    }
    if (config.globalConfig.httpsExcludeProtocols != null) {
      val protocols: Array[String] = config.globalConfig.httpsExcludeProtocols.toArray(
        new Array[String](config.globalConfig.httpsExcludeProtocols.size())
      )
      sslContextFactory.setExcludeProtocols(protocols: _*)
    }
    sslContextFactory
  }

  def createJettyServer(
    lifecycle: Lifecycle,
    config: TranquilityConfig[PropertiesBasedServerConfig],
    servlet: TranquilityServlet
  ): Server =
  {
    new Server(new QueuedThreadPool(config.globalConfig.httpThreads)) withEffect { server =>

      val connectors: ListBuffer[ServerConnector] = ListBuffer[ServerConnector]()
      if (config.globalConfig.httpPortEnable) {
        connectors += createPlaintextConnector(server, config)
      }
      if (config.globalConfig.httpsPortEnable) {
        connectors += createTLSConnector(server, config)
      }
      server.setConnectors(connectors.toArray)

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
