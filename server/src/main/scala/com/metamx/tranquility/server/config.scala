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

import com.metamx.common.scala.net.curator.DiscoAnnounceConfig
import com.metamx.common.scala.net.curator.DiscoConfig
import com.metamx.tranquility.tranquilizer.Tranquilizer
import io.druid.segment.realtime.FireDepartment
import org.joda.time.Period
import org.skife.config.Config

object config
{

  val GlobalProperties = Set("http.port", "http.threads", "http.idleTimeout")

  abstract class GlobalConfig
  {
    @Config(Array("http.port"))
    def httpPort: Int = 8200

    @Config(Array("http.threads"))
    def httpThreads: Int = 8

    @Config(Array("http.idleTimeout"))
    def httpIdleTimeout: Period = new Period("PT5M")
  }

  abstract class GeneralConfig extends GlobalConfig with DiscoConfig
  {
    @Config(Array("druid.selectors.indexing.serviceName"))
    def druidIndexingServiceName: String = "druid/overlord"

    @Config(Array("task.partitions"))
    def taskPartitions: Int = 1

    @Config(Array("task.replicants"))
    def taskReplicants: Int = 1

    @Config(Array("zookeeper.connect"))
    def zookeeperConnect: String

    @Config(Array("zookeeper.timeout"))
    def zookeeperTimeout: Period = new Period("PT20S")

    @Config(Array("tranquility.maxBatchSize"))
    def tranquilityMaxBatchSize: Int = Tranquilizer.DefaultMaxBatchSize

    @Config(Array("tranquility.maxPendingBatches"))
    def tranquilityMaxPendingBatches: Int = Tranquilizer.DefaultMaxPendingBatches

    @Config(Array("tranquility.lingerMillis"))
    def tranquilityLingerMillis: Int = Tranquilizer.DefaultLingerMillis

    @Config(Array("druid.discovery.curator.path"))
    def discoPath: String = "/druid/discovery"

    override def discoAnnounce: Option[DiscoAnnounceConfig] = None
  }

  case class DataSourceConfig(
    config: GeneralConfig,
    fireDepartment: FireDepartment
  )

}
