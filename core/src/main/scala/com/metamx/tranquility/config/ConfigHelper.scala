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

package com.metamx.tranquility.config

import com.metamx.common.scala.Predef._
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidGuicer
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.tranquilizer.Tranquilizer
import io.druid.segment.realtime.FireDepartment
import java.io.InputStream
import java.util.Properties
import org.apache.curator.framework.CuratorFramework
import org.skife.config.ConfigurationObjectFactory

/**
  * Helper methods to perform shared tasks based on common configuration file semantics.
  */
object ConfigHelper
{
  /**
    * Builds configuration object from YAML file.
    *
    * This method may have the side effect of modifying system properties if the YAML file contains properties beginning
    * with "druid.extensions." - this is necessary as DruidGuicer reads the system properties to know what extensions
    * need to be loaded. If the properties have already been set elsewhere, the system properties will not be modified.
    */
  def readConfigYaml[T <: TranquilityConfig](
    in: InputStream,
    clazz: Class[T]
  ): (T, Map[String, DataSourceConfig[T]], Dict) =
  {
    val yaml = in.withFinally(_.close()) { in =>
      dict(Yaml.load(in))
    }
    val globalProperties: Dict = getAsDict(yaml, "properties")
    val globalConfig = mapConfig(globalProperties, clazz)

    for (prop <- globalProperties.filterKeys(_.startsWith("druid.extensions."))) {
      if (!sys.props.contains(prop._1)) {
        // copy properties to system as DruidGuicer reads system properties to know what extensions need to be loaded
        sys.props += (prop._1 -> prop._2.toString)
      }
    }

    val dataSources: Dict = getAsDict(yaml, "dataSources")
    val dataSourceConfigs = for ((dataSource, x) <- dataSources) yield {
      val d = dict(x)
      val dataSourceProperties = getAsDict(d, "properties")
      for (k <- globalConfig.GlobalProperties) {
        require(!dataSourceProperties.contains(k), s"Property '$k' cannot be per-dataSource (found in[$dataSource]).")
      }

      val dataSourceSpec = getAsDict(d, "spec")
      val config = mapConfig(globalProperties ++ dataSourceProperties, clazz)

      // Sanity check: two ways of providing dataSource, they must match
      val specDataSource = DruidGuicer.objectMapper.convertValue(
        normalizeJava(dataSourceSpec),
        classOf[FireDepartment]
      ).getDataSchema.getDataSource
      require(
        dataSource == specDataSource,
        s"dataSource[$dataSource] did not match spec[$specDataSource]"
      )

      (dataSource, DataSourceConfig(config, dataSource, dataSourceSpec))
    }

    (globalConfig, dataSourceConfigs, globalProperties)
  }

  private def mapConfig[A](d: Dict, clazz: Class[A]): A = {
    val properties = new Properties
    for ((k, v) <- d if v != null) {
      properties.setProperty(k, String.valueOf(v))
    }
    val configFactory = new ConfigurationObjectFactory(properties)
    configFactory.build(clazz)
  }

  private def getAsDict(d: Dict, k: String): Dict = {
    Option(d.getOrElse(k, null)).map(dict(_)).getOrElse(Dict())
  }

  def createTranquilizerScala[T <: TranquilityConfig](
    dataSourceConfig: DataSourceConfig[T],
    finagleRegistry: FinagleRegistry,
    curator: CuratorFramework,
    location: DruidLocation = null
  ): Tranquilizer[Dict] =
  {
    createTranquilizer(
      (environment, specMap) => DruidBeams.builderFromSpecScala(environment, specMap),
      dataSourceConfig,
      finagleRegistry,
      curator,
      location
    )
  }

  def createTranquilizerJava[T <: TranquilityConfig](
    dataSourceConfig: DataSourceConfig[T],
    finagleRegistry: FinagleRegistry,
    curator: CuratorFramework,
    location: DruidLocation = null
  ): Tranquilizer[java.util.Map[String, AnyRef]] =
  {
    createTranquilizer(
      (environment, specMap) => DruidBeams.builderFromSpecJava(
        environment,
        normalizeJava(specMap).asInstanceOf[java.util.Map[String, AnyRef]]
      ),
      dataSourceConfig,
      finagleRegistry,
      curator,
      location
    )
  }

  private def createTranquilizer[MessageType, T <: TranquilityConfig](
    builderMaker: (DruidEnvironment, Dict) => DruidBeams.Builder[MessageType],
    dataSourceConfig: DataSourceConfig[T],
    finagleRegistry: FinagleRegistry,
    curator: CuratorFramework,
    location: DruidLocation
  ): Tranquilizer[MessageType] =
  {
    val environment = DruidEnvironment(dataSourceConfig.config.druidIndexingServiceName)

    // Create beam for this dataSource
    var beamBuilder = builderMaker(environment, dataSourceConfig.specMap)
      .curator(curator)
      .finagleRegistry(finagleRegistry)
      .partitions(dataSourceConfig.config.taskPartitions)
      .replicants(dataSourceConfig.config.taskReplicants)
      .druidBeamConfig(dataSourceConfig.config.druidBeamConfig)

    if (location != null) {
      beamBuilder = beamBuilder.location(location)
    }

    Tranquilizer(
      beamBuilder.buildBeam(),
      dataSourceConfig.config.tranquilityMaxBatchSize,
      dataSourceConfig.config.tranquilityMaxPendingBatches,
      dataSourceConfig.config.tranquilityLingerMillis
    )
  }
}
