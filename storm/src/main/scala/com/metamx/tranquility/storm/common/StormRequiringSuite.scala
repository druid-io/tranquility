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

package com.metamx.tranquility.storm.common

import backtype.storm.LocalCluster
import backtype.storm.generated.KillOptions
import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import scala.collection.JavaConverters._

trait StormRequiringSuite extends Logging
{
  def withLocalStorm[A](f: (LocalCluster => A)): A = {
    val storm = StormRequiringSuite.sharedCluster
    def killTopology(name: String) {
      retryOnErrors(ifException[Exception] untilPeriod 60.seconds) {
        log.info("Killing topology: %s", name)
        storm.killTopologyWithOpts(name, new KillOptions() withEffect (_.set_wait_secs(0)))
      }
    }
    def getTopologies() = {
      retryOnErrors(ifException[Exception] untilPeriod 60.seconds) {
        Option(storm.getClusterInfo) map (_.get_topologies().asScala) getOrElse {
          throw new IllegalStateException("getClusterInfo returned null!")
        }
      }
    }
    def killAllTopologies() = {
      for (topology <- getTopologies()) {
        killTopology(topology.get_name())
      }
      val start = System.currentTimeMillis()
      while (getTopologies().nonEmpty && System.currentTimeMillis() < start + 60000L) {
        log.info("Waiting for topologies to die...")
        Thread.sleep(2000)
      }
      val topologies = getTopologies()
      if (topologies.nonEmpty) {
        throw new IllegalStateException("Topologies remain: %s" format topologies.map(_.get_name()))
      }
    }
    try {
      f(storm)
    }
    finally {
      killAllTopologies()
    }
  }

}

object StormRequiringSuite
{
  private lazy val sharedCluster: LocalCluster = new LocalCluster()
}
