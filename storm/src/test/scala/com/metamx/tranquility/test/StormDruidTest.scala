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

import backtype.storm.Config
import backtype.storm.task.IMetricsContext
import backtype.storm.topology.TopologyBuilder
import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.tranquility.storm.BeamBolt
import com.metamx.tranquility.storm.BeamFactory
import com.metamx.tranquility.storm.common.SimpleKryoFactory
import com.metamx.tranquility.storm.common.SimpleSpout
import com.metamx.tranquility.storm.common.StormRequiringSuite
import com.metamx.tranquility.test.StormDruidTest._
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.DruidIntegrationSuite
import com.metamx.tranquility.test.common.JulUtils
import java.{util => ju}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

object StormDruidTest
{

  def newBeamFactory(zkConnect: String, now: DateTime): BeamFactory[SimpleEvent] = {
    new BeamFactory[SimpleEvent]
    {
      override def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext) = {
        val aDifferentCurator = CuratorFrameworkFactory.newClient(
          zkConnect,
          new BoundedExponentialBackoffRetry(100, 1000, 5)
        )
        aDifferentCurator.start()
        DirectDruidTest.newBuilder(
          aDifferentCurator, new TestingTimekeeper withEffect {
            timekeeper =>
              timekeeper.now = now
          }
        ).buildBeam()
      }
    }
  }

}

@RunWith(classOf[JUnitRunner])
class StormDruidTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Storm to Druid") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        withLocalStorm {
          storm =>
            val inputs = DirectDruidTest.generateEvents(now)
            val spout = SimpleSpout.create(inputs)
            val conf = new Config
            conf.setKryoFactory(classOf[SimpleKryoFactory])
            val builder = new TopologyBuilder
            builder.setSpout("events", spout)
            builder
              .setBolt("beam", new BeamBolt[SimpleEvent](newBeamFactory(zkConnect, now)))
              .shuffleGrouping("events")
            storm.submitTopology("test", conf, builder.createTopology())
            runTestQueriesAndAssertions(
              broker, new TestingTimekeeper withEffect {
                timekeeper =>
                  timekeeper.now = now
              }
            )
        }
    }
  }

}
