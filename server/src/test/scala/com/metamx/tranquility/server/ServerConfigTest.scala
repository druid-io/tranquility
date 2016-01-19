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

package com.metamx.tranquility.server

import com.metamx.tranquility.config.ConfigHelper
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import org.joda.time.Period
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class ServerConfigTest extends FunSuite with ShouldMatchers
{
  test("readConfigYaml") {
    val (globalConfig, dataSourceConfigs, globalProperties) = ConfigHelper.readConfigYaml(
      getClass.getClassLoader.getResourceAsStream("tranquility-server.yaml"), classOf[ServerConfig]
    )

    globalConfig.httpPort should be(8080)
    globalConfig.httpThreads should be(2)

    dataSourceConfigs.keySet should be(Set("foo"))

    val fooConfig = dataSourceConfigs("foo")
    fooConfig.config.zookeeperConnect should be("zk.example.com")
    fooConfig.config.taskPartitions should be(3)
    fooConfig.config.druidBeamConfig.firehoseGracePeriod should be(new Period("PT1S"))

    val builder = DruidBeams.builderFromSpecScala(
      DruidEnvironment("overlord"),
      fooConfig.specMap
    )
    builder.config._location.get.dataSource should be("foo")
    builder.config._rollup.get.aggregators.map(_.getName) should be(Seq("count", "x"))
    builder.config._druidTuning.get.maxRowsInMemory should be(100000)
    builder.config._druidTuning.get.intermediatePersistPeriod should be(new Period("PT45S"))
    builder.config._druidTuning.get.buildV9Directly should be(true)
    builder.config._tuning.get.windowPeriod should be(new Period("PT30S"))
  }
}
