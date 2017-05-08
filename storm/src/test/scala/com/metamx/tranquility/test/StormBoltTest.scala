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

import backtype.storm.Config
import backtype.storm.task.IMetricsContext
import backtype.storm.topology.TopologyBuilder
import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.SendResult
import com.metamx.tranquility.storm.common.SimpleKryoFactory
import com.metamx.tranquility.storm.common.SimpleSpout
import com.metamx.tranquility.storm.common.StormRequiringSuite
import com.metamx.tranquility.storm.BeamBolt
import com.metamx.tranquility.storm.BeamFactory
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.JulUtils
import com.twitter.util.Future
import java.{util => ju}
import org.scalatest.FunSuite
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SimpleBeam extends Beam[SimpleEvent]
{
  override def sendAll(messages: Seq[SimpleEvent]): Seq[Future[SendResult]] = {
    SimpleBeam.buffer ++= messages
    messages.map(_ => Future(SendResult.Sent))
  }

  override def close() = Future.Done
}

object SimpleBeam
{
  val buffer = new ArrayBuffer[SimpleEvent] with mutable.SynchronizedBuffer[SimpleEvent]

  def sortedBuffer = buffer.sortBy(_.ts.getMillis).toList
}

class SimpleBeamFactory extends BeamFactory[SimpleEvent]
{
  def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext) = new SimpleBeam
}

class StormBoltTest extends FunSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  JulUtils.routeJulThroughSlf4j()

  test("Storm BeamBolt") {
    withLocalCurator {
      curator =>
        withLocalStorm {
          storm =>
            val inputs = Seq(
              new SimpleEvent(new DateTime("2010-01-01T02:03:04Z"), "what", 1, 2, 3),
              new SimpleEvent(new DateTime("2010-01-01T02:03:05Z"), "bar", 1, 2, 3)
            ).sortBy(_.ts.getMillis)
            val spout = SimpleSpout.create(inputs)
            val conf = new Config
            conf.setKryoFactory(classOf[SimpleKryoFactory])
            val builder = new TopologyBuilder
            builder.setSpout("events", spout)
            builder.setBolt("beam", new BeamBolt[SimpleEvent](new SimpleBeamFactory)).shuffleGrouping("events")
            storm.submitTopology("test", conf, builder.createTopology())
            val start = System.currentTimeMillis()
            while (SimpleBeam.sortedBuffer != inputs && System.currentTimeMillis() < start + 300000) {
              Thread.sleep(2000)
            }
            assert(SimpleBeam.sortedBuffer === inputs)
        }
    }
  }

}
