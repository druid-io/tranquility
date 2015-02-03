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

import com.fasterxml.jackson.annotation.JsonValue
import com.metamx.common.Granularity
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.beam.RoundRobinBeam
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.MultipleFieldDruidSpatialDimension
import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.metamx.tranquility.test.DruidIntegrationTest._
import com.metamx.tranquility.test.common._
import com.metamx.tranquility.typeclass.JavaObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.LongSumAggregatorFactory
import java.{util => ju}
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

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
    val tuning = ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 1, 1, 1, 1)
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
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Druid standalone") {
    withDruidStack {
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

  test("Druid standalone - Custom ObjectWriter") {
    withDruidStack {
      (curator, broker, overlord) =>
        val timekeeper = new TestingTimekeeper
        val indexing = newBuilder(curator, timekeeper).objectWriter(new JavaObjectWriter[SimpleEvent] {
          override def asBytes(obj: SimpleEvent) = throw new UnsupportedOperationException

          override def batchAsBytes(objects: ju.Iterator[SimpleEvent]) = {
            val strings = objects.asScala.map(o => Jackson.generate(o.toMap))
            val packed = "[%s]" format strings.mkString(", ")
            packed.getBytes
          }
        }).buildService()
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

  test("Samza to Druid") {
    withDruidStack {
      (curator, broker, overlord) =>

    }
  }

}
