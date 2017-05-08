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

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.flink.BeamFactory
import com.metamx.tranquility.flink.BeamSink
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.DruidIntegrationSuite
import com.metamx.tranquility.test.common.JulUtils
import java.util.concurrent.TimeUnit
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.ForkableFlinkMiniCluster
import org.apache.flink.test.util.TestBaseUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import scala.concurrent.duration.FiniteDuration

@RunWith(classOf[JUnitRunner])
class FlinkDruidTest extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite
with Logging with BeforeAndAfter
{
  var cluster: Option[ForkableFlinkMiniCluster] = None
  val parallelism                               = 4

  before {
    val cl = TestBaseUtils.startCluster(
      1,
      parallelism,
      false,
      false,
      true
    )

    cluster = Some(cl)
  }

  after {
    val timeout: FiniteDuration = new FiniteDuration(1000, TimeUnit.SECONDS)
    cluster.foreach(c => TestBaseUtils.stopCluster(c, timeout))
  }

  JulUtils.routeJulThroughSlf4j()
  test("Flink to Druid") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputs = DirectDruidTest.generateEvents(now)
        val sink = new BeamSink[SimpleEvent](new SimpleEventBeamFactory(zkConnect))

        env.fromCollection(inputs).addSink(sink)
        env.execute

        runTestQueriesAndAssertions(
          broker, new TestingTimekeeper withEffect {
            timekeeper =>
              timekeeper.now = now
          }
        )
    }
  }
}

class SimpleEventBeamFactory(zkConnect: String) extends BeamFactory[SimpleEvent]
{
  override lazy val makeBeam: Beam[SimpleEvent] = {
    val aDifferentCurator = CuratorFrameworkFactory.newClient(
      zkConnect,
      new BoundedExponentialBackoffRetry(100, 1000, 5)
    )
    aDifferentCurator.start()
    val builder = DirectDruidTest.newBuilder(
      aDifferentCurator, new TestingTimekeeper withEffect {
        timekeeper =>
          timekeeper.now = DateTime.now
      }
    )
    builder.buildBeam()
  }
}


