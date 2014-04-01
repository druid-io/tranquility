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
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.{Files, CharStreams}
import com.google.inject.Injector
import com.metamx.common.Granularity
import com.metamx.common.scala.concurrent.loggingThread
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.tranquility.beam.{RoundRobinBeam, ClusteredBeamTuning}
import com.metamx.tranquility.druid.{DruidBeams, SpecificDruidDimensions, DruidRollup, DruidGuicer, DruidEnvironment, DruidLocation}
import com.metamx.tranquility.test.traits.CuratorRequiringSpec
import com.metamx.tranquility.typeclass.Timestamper
import com.simple.simplespec.Spec
import com.twitter.finagle.Service
import com.twitter.util.{Future, Await}
import io.druid.cli.{CliBroker, GuiceRunnable, CliOverlord}
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.initialization.Initialization
import io.druid.query.aggregation.{AggregatorFactory, LongSumAggregatorFactory}
import io.druid.query.{QuerySegmentWalker, Druids}
import io.druid.server.ClientQuerySegmentWalker
import java.io.{InputStreamReader, File}
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.junit.{Ignore, Test}
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._
import scala.util.Random

class DruidBeamsIntegrationSpec extends Spec with CuratorRequiringSpec with Logging
{
  val TimeColumn = "ts"
  val TimeFormat = "posix"

  @Ignore
  trait DruidServerHandle
  {
    def injector: Injector

    def close()
  }

  @Ignore
  case class SimpleEvent(ts: DateTime, fields: Map[String, String])
  {
    @JsonValue
    def toMap = fields ++ Map(TimeColumn -> (ts.millis / 1000))
  }

  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }

  def writeConfig(configResource: String, replacements: Map[String, String]): File = {
    val stream = ClassLoader.getSystemResourceAsStream(configResource)
    val text = CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8))
    val configFile = File.createTempFile("runtime-", ".properties")
    Files.write(replacements.foldLeft(text)((a, kv) => a.replace(kv._1, kv._2)), configFile, Charsets.UTF_8)
    configFile.deleteOnExit()
    configFile
  }

  def spawnDruidServer[A <: GuiceRunnable : ClassManifest](configFile: File): DruidServerHandle = {
    val server = classManifest[A].erasure.newInstance().asInstanceOf[A]
    val serverName = server.getClass.getName
    // Would be better to have a way to easily pass Properties into the startup injector.
    System.setProperty("druid.properties.file", configFile.toString)
    server.configure(Initialization.makeStartupInjector())
    val _injector = server.makeInjector()
    val lifecycle = server.initLifecycle(_injector)
    System.clearProperty("druid.properties.file")
    log.info("Server started up: %s", serverName)
    val thread = loggingThread {
      try {
        lifecycle.join()
      }
      catch {
        case e: Throwable =>
          log.error(e, "Failed to run server: %s", serverName)
          throw e
      }
    }
    thread.start()
    new DruidServerHandle
    {
      def injector = _injector

      def close() {
        try {
          lifecycle.stop()
        }
        catch {
          case e: Throwable =>
            log.error(e, "Failed to stop lifecycle for server: %s", serverName)
        }
        thread.interrupt()
      }
    }
  }

  def withBroker[A](curator: CuratorFramework)(f: DruidServerHandle => A): A = {
    // Randomize, but don't bother checking for conflicts
    val overlordPort = new Random().nextInt(100) + 28100
    val configFile = writeConfig(
      "druid-broker.properties",
      Map(
        ":DRUIDPORT:" -> overlordPort.toString,
        ":ZKCONNECT:" -> curator.getZookeeperClient.getCurrentConnectionString
      )
    )
    val handle = spawnDruidServer[CliBroker](configFile)
    try {
      f(handle)
    }
    finally {
      handle.close()
    }
  }

  def withOverlord[A](curator: CuratorFramework)(f: DruidServerHandle => A): A = {
    // Randomize, but don't bother checking for conflicts
    val overlordPort = new Random().nextInt(100) + 28200
    val configFile = writeConfig(
      "druid-overlord.properties",
      Map(
        ":DRUIDPORT:" -> overlordPort.toString,
        ":DRUIDFORKPORT:" -> (overlordPort + 1).toString,
        ":ZKCONNECT:" -> curator.getZookeeperClient.getCurrentConnectionString
      )
    )
    val handle = spawnDruidServer[CliOverlord](configFile)
    try {
      f(handle)
    }
    finally {
      handle.close()
    }
  }

  class A
  {
    @Test
    def testEverythingWooHoo()
    {
      // The overlord servlets don't get set up properly unless the DruidGuicer is initialized first. The scary
      // message "WARNING: Multiple Servlet injectors detected. This is a warning indicating that you have more than
      // one GuiceFilter running in your web application." also appears. Might be that the two injectors are stomping
      // on each other somehow, so we need to set up the one that actually uses its servlets last?
      DruidGuicer
      withLocalCurator {
        curator =>
          curator.create().forPath("/beams")
          withBroker(curator) {
            broker =>
              withOverlord(curator) {
                overlord =>
                  val dataSource = "xxx"
                  val timekeeper = new TestingTimekeeper
                  val tuning = ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 2, 2)
                  val rollup = DruidRollup(
                    SpecificDruidDimensions(IndexedSeq("foo")),
                    IndexedSeq(new LongSumAggregatorFactory("barr", "bar")),
                    QueryGranularity.MINUTE
                  )
                  val druidEnvironment = new DruidEnvironment(
                    "druid:tranquility:indexer",
                    "druid:tranquility:firehose:%s"
                  )
                  val druidLocation = new DruidLocation(druidEnvironment, dataSource)
                  val indexing: Service[Seq[SimpleEvent], Int] = DruidBeams.builder[SimpleEvent]()
                    .curator(curator)
                    .location(druidLocation)
                    .rollup(rollup)
                    .tuning(tuning)
                    .timekeeper(timekeeper)
                    .timestampSpec(new TimestampSpec(TimeColumn, TimeFormat))
                    .beamMergeFn(beams => new RoundRobinBeam(beams.toIndexedSeq))
                    .buildService()
                  try {
                    // Need to use somewhat nowish timestamps for the timekeeper, because this is an integration test
                    // against unmodified Druid indexing, and it will use real wall clock time to make its decisions.
                    timekeeper.now = new DateTime().hourOfDay().roundFloorCopy()
                    val eventsSent = Await.result(
                      Future.collect(
                        Seq(
                          indexing(
                            Seq(
                              SimpleEvent(timekeeper.now, Map("foo" -> "hey", "bar" -> "2"))
                            )
                          ),
                          indexing(
                            Seq(
                              SimpleEvent(timekeeper.now + 1.minute, Map("foo" -> "what", "bar" -> "3"))
                            )
                          )
                        )
                      ).map(_.sum)
                    )
                    val testQueries = Seq(
                      ((walker: QuerySegmentWalker) => Druids
                        .newTimeBoundaryQueryBuilder()
                        .dataSource("xxx")
                        .build().run(walker),
                        Seq(
                          Map(
                            "timestamp" -> timekeeper.now.toString(),
                            "result" ->
                              Map(
                                "minTime" -> timekeeper.now.toString(),
                                "maxTime" -> (timekeeper.now + 1.minute).toString()
                              )
                          )
                        )),
                      ((walker: QuerySegmentWalker) => Druids
                        .newTimeseriesQueryBuilder()
                        .dataSource("xxx")
                        .granularity(QueryGranularity.NONE)
                        .intervals("0000/3000")
                        .aggregators(Seq[AggregatorFactory](new LongSumAggregatorFactory("barr", "barr")).asJava)
                        .build().run(walker),
                        Seq(
                          Map(
                            "timestamp" -> timekeeper.now.toString(),
                            "result" -> Map("barr" -> 2)
                          ),
                          Map(
                            "timestamp" -> (timekeeper.now + 1.minute).toString(),
                            "result" -> Map("barr" -> 3)
                          )
                        ))
                    )
                    val walker = broker.injector.getInstance(classOf[ClientQuerySegmentWalker])
                    val druidObjectMapper = broker.injector.getInstance(classOf[ObjectMapper])
                    // Assertions
                    eventsSent must be(2)
                    for ((queryFn, expected) <- testQueries) {
                      var got: Seq[Dict] = null
                      val start = System.currentTimeMillis()
                      while (got != expected && System.currentTimeMillis() < start + 300000L) {
                        got = Jackson.parse[Seq[Dict]](druidObjectMapper.writeValueAsBytes(queryFn(walker)))
                        if (got != expected) {
                          log.info("Query result[%s] != expected result[%s], waiting a bit...", got, expected)
                          Thread.sleep(1000)
                        }
                      }
                      got must be(expected)
                    }
                  }
                  finally {
                    Await.result(indexing.close())
                  }
              }
          }
      }

    }
  }

}
