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
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

class SamzaDruidTestBeamFactory extends BeamFactory
{
  override def makeBeam(stream: SystemStream, config: Config) = {
    val zkConnect = config.get("tranquility.zkConnect")
    val now = new DateTime(config.get("tranquility.now"))
    val curator = CuratorFrameworkFactory.newClient(
      zkConnect,
      new BoundedExponentialBackoffRetry(100, 1000, 5)
    )
    curator.start()
    DirectDruidTest.newBuilder(
      curator, new TestingTimekeeper withEffect {
        timekeeper =>
          timekeeper.now = now
      }
    ).buildBeam().asInstanceOf[Beam[Any]]
  }
}

class SamzaDruidTestTask extends StreamTask
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

class SamzaDruidTestInputSystemFactory extends SystemFactory
{
  override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    new BlockingEnvelopeMap()
    {
      override def start() {
        val now = new DateTime(config.get("tranquility.now"))
        val ssp = new SystemStreamPartition("dummyin", "dummy", new Partition(0))
        val incoming = DirectDruidTest.generateEvents(now).zipWithIndex map { case (message, index) =>
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
class SamzaDruidTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Samza to Druid") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()
        val samzaConfig = new MapConfig(
          Map(
            "job.name" -> "dummy",
            "job.factory.class" -> classOf[ThreadJobFactory].getCanonicalName,
            "task.class" -> classOf[SamzaDruidTestTask].getCanonicalName,
            "task.inputs" -> "dummyin.dummy",
            "tranquility.now" -> now.toString(),
            "tranquility.zkConnect" -> zkConnect,
            "systems.dummyin.samza.factory" -> classOf[SamzaDruidTestInputSystemFactory].getCanonicalName,
            "systems.drood.samza.factory" -> "com.metamx.tranquility.samza.BeamSystemFactory",
            "systems.drood.beam.factory" -> classOf[SamzaDruidTestBeamFactory].getCanonicalName,
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
