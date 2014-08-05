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
import com.metamx.common.scala.Logging
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.storm.{BeamBolt, BeamFactory}
import com.metamx.tranquility.test.BeamBoltTest.{SimpleBeam, SimpleBeamFactory, SimpleEvent}
import com.metamx.tranquility.test.common.{CuratorRequiringSuite, SimpleKryoFactory, SimpleSpout, StormRequiringSuite}
import com.twitter.util.Future
import java.{util => ju}
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.FunSuite
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object BeamBoltTest
{

  case class SimpleEvent(ts: DateTime, fields: Dict)
  {
    @JsonValue
    def toMap = fields ++ Map(DruidBeams.DefaultTimestampSpec.getTimestampColumn -> ts.toString())
  }

  class SimpleBeam extends Beam[SimpleEvent]
  {
    def propagate(events: Seq[SimpleEvent]) = {
      SimpleBeam.buffer ++= events
      Future.value(events.size)
    }

    def close() = Future.Done
  }

  object SimpleBeam
  {
    val buffer = new ArrayBuffer[SimpleEvent] with mutable.SynchronizedBuffer[SimpleEvent]

    def sortedBuffer = buffer.sortBy(_.ts.millis).toList
  }

  class SimpleBeamFactory extends BeamFactory[SimpleEvent]
  {
    def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext) = new SimpleBeam
  }

}

class BeamBoltTest extends FunSuite with CuratorRequiringSuite with StormRequiringSuite with Logging
{

  test("Storm BeamBolt") {
    withLocalCurator {
      curator =>
        withLocalStorm {
          storm =>
            val inputs = List(
              new SimpleEvent(new DateTime("2010-01-01T02:03:04Z"), Map("hey" -> "what")),
              new SimpleEvent(new DateTime("2010-01-01T02:03:05Z"), Map("foo" -> "bar"))
            ).sortBy(_.ts.millis)
            val spout = new SimpleSpout[SimpleEvent](inputs)
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
