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

import _root_.io.druid.data.input.InputRow
import _root_.io.druid.data.input.impl.TimestampSpec
import _root_.io.druid.query.aggregation.LongSumAggregatorFactory
import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.{TestingTimekeeper, Timekeeper}
import com.metamx.common.scala.untyped.{Dict, long}
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning, RoundRobinBeam}
import com.metamx.tranquility.druid._
import com.metamx.tranquility.server.ServerDruidTest._
import com.metamx.tranquility.server.ServerTestUtil.withTester
import com.metamx.tranquility.test.DirectDruidTest
import com.metamx.tranquility.test.common.{CuratorRequiringSuite, DruidIntegrationSuite}
import io.druid.java.util.common.granularity.{Granularities, PeriodGranularity}
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.scalatest.{FunSuite, ShouldMatchers}

import _root_.scala.reflect.runtime.universe.typeTag

class ServerDruidTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with ShouldMatchers with Logging
{
  test("Server to Druid, application/json") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val now = new DateTime().hourOfDay().roundFloorCopy()
        val timekeeper = new TestingTimekeeper withEffect (_.now = now)
        val config = DirectDruidTest.readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val beam = DruidBeams.fromConfig(config, typeTag[InputRow]).buildBeam()
        val parseSpec = DruidBeams.makeFireDepartment(config).getDataSchema.getParser.getParseSpec
        withTester(Map(DataSource -> beam), Map(DataSource -> parseSpec)) { tester =>
          val path = s"/v1/post/$DataSource"
          val body = Jackson.bytes(DirectDruidTest.generateEvents(now))
          val headers = Map("Content-Type" -> "application/json")
          tester.post(path, body, headers) {
            tester.status should be(200)
            tester.header("Content-Type") should startWith("application/json;")
            Jackson.parse[Dict](tester.bodyBytes) should be(
              Dict(
                "result" -> Dict(
                  "received" -> 3,
                  "sent" -> 2
                )
              )
            )
          }
        }
        runTestQueriesAndAssertions(broker, timekeeper)
    }
  }

  test("Server to Druid, text/plain") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val now = new DateTime().hourOfDay().roundFloorCopy()
        val timekeeper = new TestingTimekeeper withEffect (_.now = now)
        val config = DirectDruidTest.readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val beam = DruidBeams.fromConfig(config, typeTag[InputRow]).buildBeam()
        val parseSpec = DruidBeams.makeFireDepartment(config).getDataSchema.getParser.getParseSpec
        withTester(Map(DataSource -> beam), Map(DataSource -> parseSpec)) { tester =>
          val path = s"/v1/post/$DataSource"
          val body = DirectDruidTest.generateEvents(now).map(_.toCsv).mkString("\n").getBytes(Charsets.UTF_8)
          val headers = Map("Content-Type" -> "text/plain")
          tester.post(path, body, headers) {
            tester.status should be(200)
            tester.header("Content-Type") should startWith("application/json;")
            Jackson.parse[Dict](tester.bodyBytes) should be(
              Dict(
                "result" -> Dict(
                  "received" -> 3,
                  "sent" -> 2
                )
              )
            )
          }
        }
        runTestQueriesAndAssertions(broker, timekeeper)
    }
  }
}

object ServerDruidTest
{
  val DataSource = "xxx"
  val TimeColumn = "ts"
  val TimeFormat = "posix"

  def newDruidBeam(curator: CuratorFramework, timekeeper: Timekeeper): Beam[Dict] = {
    val tuning = ClusteredBeamTuning(
      Granularities.HOUR.asInstanceOf[PeriodGranularity],
      0.minutes,
      10.minutes,
      1,
      1,
      1,
      1
    )
    val rollup = DruidRollup(
      SpecificDruidDimensions(
        Vector("foo"),
        Vector(MultipleFieldDruidSpatialDimension("coord.geo", Seq("lat", "lon")))
      ),
      IndexedSeq(new LongSumAggregatorFactory("barr", "bar")),
      Granularities.MINUTE,
      true
    )
    val druidEnvironment = DruidEnvironment.create(
      "druid/tranquility/indexer" /* Slashes should be converted to colons */
    )
    val druidLocation = new DruidLocation(druidEnvironment, DataSource)
    val timeFn = (d: Dict) => new DateTime(1000L * long(d(TimeColumn)))
    DruidBeams.builder[Dict](timeFn)
      .curator(curator)
      .location(druidLocation)
      .rollup(rollup)
      .tuning(tuning)
      .timekeeper(timekeeper)
      .timestampSpec(new TimestampSpec(TimeColumn, TimeFormat, null))
      .beamMergeFn(beams => new RoundRobinBeam(beams.toIndexedSeq))
      .buildBeam()
  }
}
