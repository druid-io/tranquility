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

import _root_.io.druid.data.input.impl.TimestampSpec
import _root_.io.druid.query.aggregation.LongSumAggregatorFactory
import _root_.scala.collection.JavaConverters._
import _root_.scala.reflect.runtime.universe.typeTag
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.metamx.common.Granularity
import com.metamx.common.ISE
import com.metamx.common.parsers.ParseException
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.beam.RoundRobinBeam
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeamConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.MultipleFieldDruidSpatialDimension
import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.metamx.tranquility.druid.TaskLocator
import com.metamx.tranquility.test.DirectDruidTest._
import com.metamx.tranquility.test.common._
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.metamx.tranquility.typeclass.DefaultJsonWriter
import com.metamx.tranquility.typeclass.JavaObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.NonFatal
import com.twitter.util.Return
import com.twitter.util.Throw
import _root_.io.druid.granularity.QueryGranularities
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.{util => ju}
import javax.ws.rs.core.MediaType
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite

object DirectDruidTest
{
  val TimeColumn = "ts"
  val TimeFormat = "posix"

  def generateEvents(now: DateTime): Seq[SimpleEvent] = {
    // Need to use somewhat nowish timestamps for the timekeeper, because this is an integration test
    // against unmodified Druid indexing, and it will use real wall clock time to make its decisions.
    Seq(
      // This event should be sent
      SimpleEvent(now, "hey", 2, 37.7833, -122.4167),

      // This event is intended to be dropped
      SimpleEvent(now - 1.year, "hey", 4, 37.7833, -122.4167),

      // This event should be sent
      SimpleEvent(now + 1.minute, "what", 3, 37.7833, 122.4167)
    )
  }

  def newBuilder(curator: CuratorFramework, timekeeper: Timekeeper): DruidBeams.Builder[SimpleEvent, SimpleEvent] = {
    val dataSource = "xxx"
    val tuning = ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 1, 1, 1, 1)
    val rollup = DruidRollup(
      SpecificDruidDimensions(
        Vector("foo"),
        Vector(MultipleFieldDruidSpatialDimension("coord.geo", Seq("lat", "lon")))
      ),
      IndexedSeq(new LongSumAggregatorFactory("barr", "bar")),
      QueryGranularities.MINUTE,
      true
    )
    val druidEnvironment = new DruidEnvironment(
      "druid/tranquility/indexer" /* Slashes should be converted to colons */ ,
      "druid:tranquility:firehose:%s"
    )
    val druidLocation = new DruidLocation(druidEnvironment, dataSource)
    DruidBeams.builder[SimpleEvent]()
      .curator(curator)
      .discoveryPath("/disco-fever")
      .location(druidLocation)
      .rollup(rollup)
      .tuning(tuning)
      .timekeeper(timekeeper)
      .timestampSpec(new TimestampSpec(TimeColumn, TimeFormat, null))
      .beamMergeFn(beams => new RoundRobinBeam(beams.toIndexedSeq))
  }

  def readDataSourceConfig(
    zkConnect: String,
    rsrc: String = "direct-druid-test.yaml"
  ): DataSourceConfig[PropertiesBasedConfig] =
  {
    val configString = new String(
      ByteStreams.toByteArray(getClass.getClassLoader.getResourceAsStream(rsrc)),
      Charsets.UTF_8
    ).replaceAll("@ZKPLACEHOLDER@", zkConnect)
    val config = TranquilityConfig.read(new ByteArrayInputStream(configString.getBytes(Charsets.UTF_8)))
    config.getDataSource("xxx")
  }
}

class DirectDruidTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Druid standalone") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val indexing = newBuilder(curator, timekeeper).buildTranquilizer()
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - Custom ObjectWriter") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val beam = newBuilder(curator, timekeeper).objectWriter(
          new JavaObjectWriter[SimpleEvent]
          {
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
          }
        ).buildBeam()
        val indexing = Tranquilizer.create(beam)
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Smile mapper") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(
          curator.getZookeeperClient.getCurrentConnectionString,
          "direct-druid-test-smile.yaml"
        )
        val indexing = DruidBeams
          .fromConfig(config)
          .timekeeper(timekeeper)
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event.toMap.asJava.asInstanceOf[ju.Map[String, AnyRef]]) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Custom type") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, implicitly[Timestamper[SimpleEvent]], new DefaultJsonWriter)
          .timekeeper(timekeeper)
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - overlord based task discovery") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val indexing = newBuilder(curator, timekeeper)
          .druidBeamConfig(DruidBeamConfig(taskLocator = TaskLocator.Overlord))
          .timekeeper(timekeeper)
          .buildTranquilizer()
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Java Map type") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, typeTag[java.util.Map[String, AnyRef]])
          .timekeeper(timekeeper)
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event.toMap.asJava.asInstanceOf[java.util.Map[String, AnyRef]]) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Scala Map type") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, typeTag[Map[String, Any]])
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now) map { event =>
              indexing.send(event.toMap) transform {
                case Return(()) => Future.value(true)
                case Throw(e: MessageDroppedException) => Future.value(false)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(true, false, true))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - String type (CSV with ParseExceptions)") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, typeTag[String])
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            (generateEvents(timekeeper.now).map(_.toCsv) ++ Seq("invalid \"csv")) map { csv =>
              indexing.send(csv) transform {
                case Return(()) => Future.value(1)
                case Throw(e: MessageDroppedException) => Future.value(2)
                case Throw(e: ParseException) => Future.value(3)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(1, 2, 1, 3))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Array[Byte] type (CSV with ParseExceptions)") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, typeTag[Array[Byte]])
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            (generateEvents(timekeeper.now).map(_.toCsv) ++ Seq("invalid \"csv")) map { csv =>
              indexing.send(csv.getBytes(Charsets.UTF_8)) transform {
                case Return(()) => Future.value(1)
                case Throw(e: MessageDroppedException) => Future.value(2)
                case Throw(e: ParseException) => Future.value(3)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(1, 2, 1, 3))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - ByteBuffer type (CSV with ParseExceptions)") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(curator.getZookeeperClient.getCurrentConnectionString)
        val indexing = DruidBeams
          .fromConfig(config, typeTag[ByteBuffer])
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            (generateEvents(timekeeper.now).map(_.toCsv) ++ Seq("invalid \"csv")) map { csv =>
              indexing.send(ByteBuffer.wrap(csv.getBytes(Charsets.UTF_8))) transform {
                case Return(()) => Future.value(1)
                case Throw(e: MessageDroppedException) => Future.value(2)
                case Throw(e: ParseException) => Future.value(3)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(1, 2, 1, 3))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }

  test("Druid standalone - From config file - Array[Byte] type (JSON with flattenSpec)") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val timekeeper = new TestingTimekeeper
        val config = readDataSourceConfig(
          curator.getZookeeperClient.getCurrentConnectionString,
          "direct-druid-test-flattenSpec.yaml"
        )
        val indexing = DruidBeams
          .fromConfig(config, typeTag[Array[Byte]])
          .buildTranquilizer(config.tranquilizerBuilder())
        indexing.start()
        try {
          timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
          val eventsSent = Future.collect(
            generateEvents(timekeeper.now).map(_.toNestedMap) map { m =>
              indexing.send(Jackson.bytes(m)) transform {
                case Return(()) => Future.value(1)
                case Throw(e: MessageDroppedException) => Future.value(2)
                case Throw(e) => Future.exception(e)
              }
            }
          )
          assert(Await.result(eventsSent) === Seq(1, 2, 1))
          runTestQueriesAndAssertions(broker, timekeeper)
        }
        catch {
          case NonFatal(e) =>
            throw new ISE(e, "Failed test")
        }
        finally {
          indexing.stop()
        }
    }
  }
}
