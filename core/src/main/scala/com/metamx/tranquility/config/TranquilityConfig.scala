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

import com.metamx.common.IAE
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.Yaml
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.untyped._
import java.io.InputStream
import scala.collection.JavaConverters._

case class TranquilityConfig[ConfigType <: PropertiesBasedConfig](
  globalConfig: ConfigType,
  dataSourceConfigs: Map[String, DataSourceConfig[ConfigType]]
)
{
  validate()

  def getDataSources: java.util.List[String] = {
    dataSourceConfigs.keys.toIndexedSeq.asJava
  }

  def getDataSource(dataSource: String): DataSourceConfig[ConfigType] = {
    dataSourceConfigs.getOrElse(dataSource, null)
  }

  private def validate(): Unit = {
    for ((dataSource, config) <- dataSourceConfigs) {
      require(
        dataSource == config.dataSource,
        "dataSource key[%s] must match value[%s]" format(dataSource, config.dataSource)
      )
    }
  }
}

object TranquilityConfig
{
  def read[ConfigType <: PropertiesBasedConfig](
    in: InputStream,
    clazz: Class[ConfigType]
  ): TranquilityConfig[ConfigType] =
  {
    val yaml = in.withFinally(_.close()) { in =>
      dict(Yaml.load(in))
    }

    val globalProperties = getAsDict(yaml, "properties")
    val globalConfig = PropertiesBasedConfig.fromDict(globalProperties, clazz)

    val dataSources: Dict = getDataSourcesUntyped(yaml)
    val dataSourceConfigs: Map[String, DataSourceConfig[ConfigType]] = for ((dataSource, x) <- dataSources) yield {
      val d = dict(x)
      val dataSourceProperties = getAsDict(d, "properties")
      for (k <- globalConfig.globalPropertyNames) {
        require(!dataSourceProperties.contains(k), s"Property '$k' cannot be per-dataSource (found in[$dataSource]).")
      }

      val dataSourceSpec = getAsDict(d, "spec")
      val dataSourcePropertiesBasedConfig = PropertiesBasedConfig.fromDict(
        globalProperties ++ dataSourceProperties,
        clazz
      )

      (dataSource, DataSourceConfig(dataSource, dataSourcePropertiesBasedConfig, dataSourceSpec))
    }

    TranquilityConfig(globalConfig, dataSourceConfigs)
  }

  def read(
    in: InputStream
  ): TranquilityConfig[PropertiesBasedConfig] =
  {
    read(in, classOf[PropertiesBasedConfig])
  }

  private def getAsDict(d: Dict, k: String): Dict = {
    Option(d.getOrElse(k, null)).map(dict(_)).getOrElse(Dict())
  }

  private def getDataSourcesUntyped[ConfigType <: PropertiesBasedConfig](
    yaml: Dict
  ): Map[String, Dict] =
  {
    val dataSourcesUntyped: Map[String, Dict] = Option(yaml.getOrElse("dataSources", null)).map(normalize(_)) match {
      case None =>
        Map.empty

      case Some(d: Map[_, _]) =>
        dict(d).strictMapValues(dict(_))

      case Some(ds: Seq[_]) =>
        (ds.map(dict(_)) map { d =>
          val dataSourceName = try {
            str(getAsDict(getAsDict(d, "spec"), "dataSchema")("dataSource"))
          }
          catch {
            case e: Exception =>
              throw new IAE(e, "Missing 'spec' -> 'dataSchema' -> 'dataSource' for one of the dataSources")
          }
          dataSourceName -> d
        }).toMap

      case Some(x) =>
        throw new IAE(s"dataSources must be a list or map")
    }

    dataSourcesUntyped
  }
}
