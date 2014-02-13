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
import backtype.storm.serialization.IKryoFactory
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.{TopologyContext, IMetricsContext}
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.topology.{OutputFieldsDeclarer, TopologyBuilder}
import backtype.storm.tuple.Fields
import com.esotericsoftware.kryo.Kryo
import com.fasterxml.jackson.annotation.JsonValue
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.storm.{BeamBolt, BeamFactory}
import com.metamx.tranquility.test.StormIntegrationSpec.{SimpleKryoFactory, SimpleBeam, SimpleBeamFactory, SimpleEvent}
import com.metamx.tranquility.test.traits.{StormRequiringSpec, CuratorRequiringSpec}
import com.simple.simplespec.Spec
import com.twitter.chill.{KryoSerializer, KryoBase}
import com.twitter.util.Future
import java.{util => ju}
import org.joda.time.DateTime
import org.junit.{Ignore, Test}
import org.objenesis.strategy.StdInstantiatorStrategy
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StormIntegrationSpec
{

  @Ignore
  case class SimpleEvent(ts: DateTime, fields: Map[String, String])
  {
    @JsonValue
    def toMap = fields ++ Map(DruidRollup.DefaultTimestampColumn -> ts.toString())
  }

  @Ignore
  class SimpleBeam extends Beam[SimpleEvent]
  {
    def propagate(events: Seq[SimpleEvent]) = {
      SimpleBeam.buffer ++= events
      Future.value(events.size)
    }

    def close() = Future.Done
  }

  @Ignore
  object SimpleBeam
  {
    val buffer = new ArrayBuffer[SimpleEvent] with mutable.SynchronizedBuffer[SimpleEvent]

    def sortedBuffer = buffer.sortBy(_.ts.millis).toList
  }

  @Ignore
  class SimpleBeamFactory extends BeamFactory[SimpleEvent]
  {
    def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext) = new SimpleBeam
  }

  @Ignore
  class SimpleKryoFactory extends IKryoFactory
  {
    def getKryo(conf: ju.Map[_, _]) = {
      new KryoBase withEffect {
        kryo =>
          kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
          kryo.setRegistrationRequired(false)
          KryoSerializer.registerAll(kryo)
          kryo.register(Nil.getClass)
          kryo.register(classOf[scala.collection.immutable.::[_]])
          kryo.register(classOf[scala.collection.immutable.List[_]])
      }
    }

    def preRegister(kryo: Kryo, conf: ju.Map[_, _]) {}

    def postRegister(kryo: Kryo, conf: ju.Map[_, _]) {}

    def postDecorate(kryo: Kryo, conf: ju.Map[_, _]) {}
  }

}

class StormIntegrationSpec extends Spec with CuratorRequiringSpec with StormRequiringSpec with Logging
{

  class A
  {
    @Test
    def testEverythingWooHoo() {
      withLocalCurator {
        curator =>
          withLocalStorm {
            storm =>
              val inputs = List(
                new SimpleEvent(new DateTime("2010-01-01T02:03:04Z"), Map("hey" -> "what")),
                new SimpleEvent(new DateTime("2010-01-01T02:03:05Z"), Map("foo" -> "bar"))
              ).sortBy(_.ts.millis)
              val spout = new BaseRichSpout {
                var collector: SpoutOutputCollector = null
                var buf                             = inputs

                override def open(conf: ju.Map[_, _], context: TopologyContext, _collector: SpoutOutputCollector) {
                  collector = _collector
                }

                override def declareOutputFields(declarer: OutputFieldsDeclarer) {
                  declarer.declare(new Fields("event"))
                }

                override def nextTuple() = {
                  buf match {
                    case x :: xs =>
                      collector.emit(List[AnyRef](x).asJava)
                      buf = xs
                    case Nil =>
                  }
                }
              }
              val conf = new Config
              conf.setKryoFactory(classOf[SimpleKryoFactory])
              val builder = new TopologyBuilder
              builder.setSpout("events", spout)
              builder.setBolt("beam", new BeamBolt[SimpleEvent](new SimpleBeamFactory, None)).shuffleGrouping("events")
              storm.submitTopology("test", conf, builder.createTopology())
              val start = System.currentTimeMillis()
              while (SimpleBeam.sortedBuffer != inputs && System.currentTimeMillis() < start + 300000) {
                Thread.sleep(2000)
              }
              SimpleBeam.sortedBuffer must be(inputs)
          }
      }
    }
  }

}
