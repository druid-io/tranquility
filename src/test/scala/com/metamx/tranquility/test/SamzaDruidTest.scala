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

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.samza.BeamFactory
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.DruidIntegrationSuite
import com.metamx.tranquility.test.common.JulUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.job.JobRunner
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.util.BlockingEnvelopeMap
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

class SamzaIntegrationTestBeamFactory extends BeamFactory
{
  override def makeBeam(stream: SystemStream, config: Config) = {
    val zkConnect = config.get("tranquility.zkConnect")
    val now = new DateTime(config.get("tranquility.now"))
    val curator = CuratorFrameworkFactory.newClient(
      zkConnect,
      new BoundedExponentialBackoffRetry(100, 1000, 5)
    )
    curator.start()
    DruidIntegrationTest.newBuilder(
      curator, new TestingTimekeeper withEffect {
        timekeeper =>
          timekeeper.now = now
      }
    ).buildBeam().asInstanceOf[Beam[Any]]
  }
}

class SamzaIntegrationTestTask extends StreamTask
{
  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit =
  {
    collector.send(
      new OutgoingMessageEnvelope(
        new SystemStream("drood", "xxx"),
        envelope.getKey,
        envelope.getMessage
      )
    )
  }
}

class SamzaIntegrationTestInputSystemFactory extends SystemFactory
{
  override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    new BlockingEnvelopeMap()
    {
      override def start() {
        val now = new DateTime(config.get("tranquility.now"))
        val ssp = new SystemStreamPartition("dummyin", "dummy", new Partition(0))
        val incoming = DruidIntegrationTest.generateEvents(now).zipWithIndex map { case (message, index) =>
          new IncomingMessageEnvelope(ssp, index.toString, null, message)
        }
        putAll(ssp, incoming.asJava)
      }

      override def stop() {}
    }
  }

  override def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new UnsupportedOperationException
  }

  override def getAdmin(systemName: String, config: Config) = {
    new SinglePartitionWithoutOffsetsSystemAdmin
  }
}

@RunWith(classOf[JUnitRunner])
class SamzaIntegrationTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Samza to Druid") {
    withDruidStack {
      (curator, broker, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        val samzaConfig = new MapConfig(
          Map(
            "job.name" -> "dummy",
            "job.factory.class" -> classOf[ThreadJobFactory].getCanonicalName,
            "task.class" -> classOf[SamzaIntegrationTestTask].getCanonicalName,
            "task.inputs" -> "dummyin.dummy",
            "tranquility.now" -> now.toString(),
            "tranquility.zkConnect" -> zkConnect,
            "systems.dummyin.samza.factory" -> classOf[SamzaIntegrationTestInputSystemFactory].getCanonicalName,
            "systems.drood.samza.factory" -> "com.metamx.tranquility.samza.BeamSystemFactory",
            "systems.drood.beam.factory" -> classOf[SamzaIntegrationTestBeamFactory].getCanonicalName,
            "systems.drood.beam.batchSize" -> "1"
          ).asJava
        )

        new JobRunner(samzaConfig).run()
        runTestQueriesAndAssertions(
          broker,
          new TestingTimekeeper withEffect {
            timekeeper =>
              timekeeper.now = now
          }
        )
    }
  }

}
