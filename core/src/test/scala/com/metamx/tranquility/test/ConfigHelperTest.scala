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

import com.metamx.tranquility.config.ConfigHelper
import com.metamx.tranquility.config.TranquilityConfig
import org.joda.time.Period
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class ConfigHelperTest extends FunSuite with ShouldMatchers
{
  test("readConfigYaml") {
    val (globalConfig, dataSourceConfigs, globalProperties) = ConfigHelper.readConfigYaml(
      getClass.getClassLoader.getResourceAsStream("tranquility-core.yaml"), classOf[TranquilityConfig]
    )

    dataSourceConfigs.keySet should be(Set("foo"))

    val fooConfig = dataSourceConfigs("foo")
    fooConfig.fireDepartment.getDataSchema.getDataSource should be("foo")
    fooConfig.fireDepartment.getDataSchema.getAggregators.map(_.getName).toList should be(Seq("count", "x"))
    fooConfig.fireDepartment.getTuningConfig.getMaxRowsInMemory should be(100000)
    fooConfig.fireDepartment.getTuningConfig.getIntermediatePersistPeriod should be(new Period("PT45S"))
    fooConfig.fireDepartment.getTuningConfig.getWindowPeriod should be(new Period("PT30S"))
    fooConfig.config.zookeeperConnect should be("zk.example.com")
    fooConfig.config.taskPartitions should be(3)
    fooConfig.config.druidBeamConfig.firehoseGracePeriod should be(new Period("PT5M"))
  }
}
