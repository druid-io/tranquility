/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.metamx.tranquility.test.DirectDruidTest._
import com.metamx.tranquility.test.common._
import com.metamx.tranquility.typeclass.JavaObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.LongSumAggregatorFactory
import java.{util => ju}
import javax.ws.rs.core.MediaType
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

object DirectDruidTest
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

class DirectDruidTest
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

          /**
           * @return content type of the serialized form
           */
          override def contentType: String = MediaType.APPLICATION_JSON
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

}
