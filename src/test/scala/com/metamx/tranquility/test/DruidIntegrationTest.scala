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
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.{CharStreams, Files}
import com.google.inject.Injector
import com.metamx.common.Granularity
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.concurrent.loggingThread
import com.metamx.common.scala.timekeeper.{TestingTimekeeper, Timekeeper}
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.tranquility.beam.{ClusteredBeamTuning, RoundRobinBeam}
import com.metamx.tranquility.druid.{DruidBeams, DruidEnvironment, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.metamx.tranquility.storm.{BeamBolt, BeamFactory}
import com.metamx.tranquility.test.DruidIntegrationTest._
import com.metamx.tranquility.test.common._
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.{Await, Future}
import io.druid.cli.{CliBroker, CliOverlord, GuiceRunnable}
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.initialization.Initialization
import io.druid.query.aggregation.{AggregatorFactory, LongSumAggregatorFactory}
import io.druid.query.{Druids, QuerySegmentWalker}
import io.druid.server.ClientQuerySegmentWalker
import java.io.{File, InputStreamReader}
import java.{util => ju}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.JavaConverters._
import scala.util.Random

object DruidIntegrationTest
{
  val TimeColumn = "ts"
  val TimeFormat = "posix"

  def generateEvents(now: DateTime): Seq[SimpleEvent] = {
    // Need to use somewhat nowish timestamps for the timekeeper, because this is an integration test
    // against unmodified Druid indexing, and it will use real wall clock time to make its decisions.
    Seq(
      SimpleEvent(now, Map("foo" -> "hey", "bar" -> "2")),
      SimpleEvent(now + 1.minute, Map("foo" -> "what", "bar" -> "3"))
    )
  }

  def newBuilder(curator: CuratorFramework, timekeeper: Timekeeper): DruidBeams.Builder[SimpleEvent] = {
    val dataSource = "xxx"
    val tuning = ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 1, 1)
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

  case class SimpleEvent(ts: DateTime, fields: Map[String, String])
  {
    @JsonValue
    def toMap = fields ++ Map(TimeColumn -> (ts.millis / 1000))
  }

  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }
}

class DruidIntegrationTest
  extends FunSuite with CuratorRequiringSuite with StormRequiringSuite with Logging with BeforeAndAfter
{

  trait DruidServerHandle
  {
    def injector: Injector

    def close()
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

  def runTestQueriesAndAssertions(druidServer: DruidServerHandle, timekeeper: Timekeeper) {
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
        .granularity(QueryGranularity.MINUTE)
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
    val walker = druidServer.injector.getInstance(classOf[ClientQuerySegmentWalker])
    val druidObjectMapper = druidServer.injector.getInstance(classOf[ObjectMapper])
    // Assertions
    for ((queryFn, expected) <- testQueries) {
      var got: Seq[Dict] = null
      val start = System.currentTimeMillis()
      while (got != expected && System.currentTimeMillis() < start + 300000L) {
        got = Jackson.parse[Seq[Dict]](druidObjectMapper.writeValueAsBytes(queryFn(walker)))
        val gotAsString = got.toString match {
          case x if x.size > 1024 => x.take(1024) + " ..."
          case x => x
        }
        if (got != expected) {
          log.info("Query result[%s] != expected result[%s], waiting a bit...", gotAsString, expected)
          Thread.sleep(1000)
        }
      }
      assert(got === expected)
    }
  }

  test("DruidStandalone")
  {
    withLocalCurator {
      curator =>
        curator.create().forPath("/beams")
        withBroker(curator) {
          broker =>
            withOverlord(curator) {
              overlord =>
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
    }
  }

  test("StormToDruid")
  {
    withLocalCurator {
      curator =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        curator.create().forPath("/beams")
        withBroker(curator) {
          broker =>
            withOverlord(curator) {
              overlord =>
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
  }

}
