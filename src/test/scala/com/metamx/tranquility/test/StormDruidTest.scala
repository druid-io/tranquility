/*
 * Tranquility.
 * Copyright (C) 2013, 2014, 2015  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.tranquility.test

import backtype.storm.Config
import backtype.storm.task.IMetricsContext
import backtype.storm.topology.TopologyBuilder
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.tranquility.storm.BeamBolt
import com.metamx.tranquility.storm.BeamFactory
import com.metamx.tranquility.test.DruidIntegrationTest.SimpleEvent
import com.metamx.tranquility.test.StormIntegrationTest._
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.DruidIntegrationSuite
import com.metamx.tranquility.test.common.JulUtils
import com.metamx.tranquility.test.common.SimpleKryoFactory
import com.metamx.tranquility.test.common.SimpleSpout
import com.metamx.tranquility.test.common.StormRequiringSuite
import java.{util => ju}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

object StormIntegrationTest
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
        DruidIntegrationTest.newBuilder(
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
class StormIntegrationTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Storm to Druid") {
    withDruidStack {
      (curator, broker, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        withLocalStorm {
          storm =>
            val inputs = DruidIntegrationTest.generateEvents(now)
            val spout = new SimpleSpout[SimpleEvent](inputs)
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