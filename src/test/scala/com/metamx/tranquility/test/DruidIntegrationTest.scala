/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
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
import com.fasterxml.jackson.annotation.JsonValue
import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.{TestingTimekeeper, Timekeeper}
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.beam.{ClusteredBeamTuning, RoundRobinBeam}
import com.metamx.tranquility.druid.{DruidBeams, DruidEnvironment, DruidLocation, DruidRollup, MultipleFieldDruidSpatialDimension, SpecificDruidDimensions}
import com.metamx.tranquility.storm.{BeamBolt, BeamFactory}
import com.metamx.tranquility.test.DruidIntegrationTest._
import com.metamx.tranquility.test.common._
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.{Await, Future}
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.LongSumAggregatorFactory
import java.{util => ju}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.FunSuite

object DruidIntegrationTest
{
  val TimeColumn = "ts"
  val TimeFormat = "posix"

  def generateEvents(now: DateTime): Seq[SimpleEvent] = {
    // Need to use somewhat nowish timestamps for the timekeeper, because this is an integration test
    // against unmodified Druid indexing, and it will use real wall clock time to make its decisions.
    Seq(
      SimpleEvent(now, Map("foo" -> "hey", "bar" -> "2", "lat" -> "37.7833", "lon" -> "-122.4167")),
      SimpleEvent(now + 1.minute, Map("foo" -> "what", "bar" -> "3", "lat" -> "37.7833", "lon" -> "122.4167"))
    )
  }

  def newBuilder(curator: CuratorFramework, timekeeper: Timekeeper): DruidBeams.Builder[SimpleEvent] = {
    val dataSource = "xxx"
    val tuning = ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 1, 1)
    val rollup = DruidRollup(
      SpecificDruidDimensions(
        Vector("foo"),
        Vector(MultipleFieldDruidSpatialDimension("coord.geo", Seq("lat", "lon")))
      ),
      IndexedSeq(new LongSumAggregatorFactory("barr", "bar")),
      QueryGranularity.MINUTE
    )
    val druidEnvironment = new DruidEnvironment(
      "druid/tranquility/indexer" /* Slashes should be converted to colons */ ,
      "druid:tranquility:firehose:%s"
    )
    val druidLocation = new DruidLocation(druidEnvironment, dataSource)
    DruidBeams.builder[SimpleEvent]()
      .curator(curator)
      .location(druidLocation)
      .rollup(rollup)
      .tuning(tuning)
      .timekeeper(timekeeper)
      .timestampSpec(new TimestampSpec(TimeColumn, TimeFormat))
      .beamMergeFn(beams => new RoundRobinBeam(beams.toIndexedSeq))
  }

  def newBeamFactory(zkConnect: String, now: DateTime): BeamFactory[SimpleEvent] = {
    new BeamFactory[SimpleEvent]
    {
      override def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext) = {
        val aDifferentCurator = CuratorFrameworkFactory.newClient(
          zkConnect,
          new BoundedExponentialBackoffRetry(100, 1000, 5)
        )
        aDifferentCurator.start()
        newBuilder(
          aDifferentCurator, new TestingTimekeeper withEffect {
            timekeeper =>
              timekeeper.now = now
          }
        ).buildBeam()
      }
    }
  }

  case class SimpleEvent(ts: DateTime, fields: Dict)
  {
    @JsonValue
    def toMap = fields ++ Map(TimeColumn -> (ts.millis / 1000))
  }

  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }
}

class DruidIntegrationTest
  extends DruidIntegrationSuite with FunSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  test("DruidStandalone") {
    withTestStack {
      (curator, broker, overlord) =>
        val timekeeper = new TestingTimekeeper
        val indexing = newBuilder(curator, timekeeper).buildService()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Await.result(
            Future.collect(
              generateEvents(timekeeper.now).map(x => indexing(Seq(x)))
            ).map(_.sum)
          )
          assert(eventsSent === 2)
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        finally {
          Await.result(indexing.close())
        }
    }
  }

  test("StormToDruid") {
    withTestStack {
      (curator, broker, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        withLocalStorm {
          storm =>
            val inputs = generateEvents(now)
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
