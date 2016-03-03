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

package com.metamx.tranquility.test

import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.typeclass.DefaultJsonWriter
import com.metamx.tranquility.typeclass.Timestamper
import org.joda.time.DateTime
import org.joda.time.Period
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class TranquilityConfigTest extends FunSuite with ShouldMatchers
{
  val DataSource = "foo"

  test("readConfigYaml: YAML") {
    val config = TranquilityConfig.read(
      getClass.getClassLoader.getResourceAsStream("tranquility-core.yaml"),
      classOf[PropertiesBasedConfig]
    )

    val dataSourceConfigs = config.dataSourceConfigs
    dataSourceConfigs.keySet should be(Set(DataSource))

    val fooConfig = dataSourceConfigs(DataSource)
    fooConfig.propertiesBasedConfig.zookeeperConnect should be("zk.example.com")
    fooConfig.propertiesBasedConfig.taskPartitions should be(3)
    fooConfig.propertiesBasedConfig.druidBeamConfig.firehoseGracePeriod should be(new Period("PT5M"))

    for (builder <- makeBuilders(fooConfig.specMap)) {
      builder.config._location.get.dataSource should be(DataSource)
      builder.config._rollup.get.aggregators.map(_.getName) should be(Seq("count", "x"))
      builder.config._druidTuningMap.get should be(Dict(
        "type" -> "realtime",
        "maxRowsInMemory" -> 100000,
        "buildV9Directly" -> true,
        "intermediatePersistPeriod" -> "PT45S",
        "windowPeriod" -> "PT30S",
        "maxPendingPersists" -> 0
      ))
      builder.config._tuning.get.windowPeriod should be(new Period("PT30S"))
    }
  }

  test("readConfigYaml: JSON") {
    val config = TranquilityConfig.read(
      getClass.getClassLoader.getResourceAsStream("tranquility-core.json"),
      classOf[PropertiesBasedConfig]
    )

    val dataSourceConfigs = config.dataSourceConfigs
    dataSourceConfigs.keySet should be(Set(DataSource))

    val fooConfig = dataSourceConfigs(DataSource)
    fooConfig.propertiesBasedConfig.zookeeperConnect should be("zk.example.com")
    fooConfig.propertiesBasedConfig.taskPartitions should be(3)
    fooConfig.propertiesBasedConfig.druidBeamConfig.firehoseGracePeriod should be(new Period("PT5M"))

    for (builder <- makeBuilders(fooConfig.specMap)) {
      builder.config._location.get.dataSource should be(DataSource)
      builder.config._rollup.get.aggregators.map(_.getName) should be(Seq("count", "x"))
      builder.config._druidTuningMap.get should be(Dict(
        "type" -> "realtime",
        "maxRowsInMemory" -> 100000,
        "buildV9Directly" -> true,
        "intermediatePersistPeriod" -> "PT45S",
        "windowPeriod" -> "PT30S",
        "maxPendingPersists" -> 0
      ))
      builder.config._tuning.get.windowPeriod should be(new Period("PT30S"))
    }
  }

  test("readConfigYaml: JSON, dataSources as list") {
    val config = TranquilityConfig.read(
      getClass.getClassLoader.getResourceAsStream("tranquility-core-dataSources-as-list.json"),
      classOf[PropertiesBasedConfig]
    )

    val dataSourceConfigs = config.dataSourceConfigs
    dataSourceConfigs.keySet should be(Set(DataSource))

    val fooConfig = dataSourceConfigs(DataSource)
    fooConfig.propertiesBasedConfig.zookeeperConnect should be("zk.example.com")
    fooConfig.propertiesBasedConfig.taskPartitions should be(3)
    fooConfig.propertiesBasedConfig.druidBeamConfig.firehoseGracePeriod should be(new Period("PT5M"))

    for (builder <- makeBuilders(fooConfig.specMap)) {
      builder.config._location.get.dataSource should be(DataSource)
      builder.config._rollup.get.aggregators.map(_.getName) should be(Seq("count", "x"))
      builder.config._druidTuningMap.get should be(Dict(
        "type" -> "realtime",
        "maxRowsInMemory" -> 100000,
        "buildV9Directly" -> true,
        "intermediatePersistPeriod" -> "PT45S",
        "windowPeriod" -> "PT30S",
        "maxPendingPersists" -> 0
      ))
      builder.config._tuning.get.windowPeriod should be(new Period("PT30S"))
    }
  }

  test("readConfigYaml: JSON, no tuning config") {
    val config = TranquilityConfig.read(
      getClass.getClassLoader.getResourceAsStream("tranquility-core-no-tuningConfig.json"),
      classOf[PropertiesBasedConfig]
    )

    val dataSourceConfigs = config.dataSourceConfigs
    dataSourceConfigs.keySet should be(Set(DataSource))

    val fooConfig = dataSourceConfigs(DataSource)
    fooConfig.propertiesBasedConfig.zookeeperConnect should be("zk.example.com")
    fooConfig.propertiesBasedConfig.taskPartitions should be(3)
    fooConfig.propertiesBasedConfig.druidBeamConfig.firehoseGracePeriod should be(new Period("PT5M"))

    for (builder <- makeBuilders(fooConfig.specMap)) {
      builder.config._location.get.dataSource should be(DataSource)
      builder.config._rollup.get.aggregators.map(_.getName) should be(Seq("count", "x"))
      builder.config._druidTuningMap.get should be(Dict(
        "type" -> "realtime",
        "maxRowsInMemory" -> 75000,
        "buildV9Directly" -> false,
        "intermediatePersistPeriod" -> "PT10M",
        "maxPendingPersists" -> 0
      ))
      builder.config._tuning.get.windowPeriod should be(new Period("PT10M"))
    }
  }

  private def makeBuilders(
    specMap: Dict
  ): Seq[DruidBeams.Builder[_, _]] =
  {
    val config = DataSourceConfig(
      DataSource,
      new PropertiesBasedConfig()
      {
        override def zookeeperConnect: String = "buh"
      },
      specMap
    )

    val basicBuilder = DruidBeams.fromConfig(config)
    val customBuilder = DruidBeams.fromConfig[Dict](
      config,
      new Timestamper[Dict]
      {
        override def timestamp(a: Dict): DateTime = new DateTime(a("ts"))
      },
      new DefaultJsonWriter
    )
    Seq(basicBuilder, customBuilder)
  }
}
