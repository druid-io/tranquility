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
package com.metamx.tranquility.storm

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.{BeamPacketizer, BeamPacketizerListener}
import java.{util => ju}

/**
 * A Storm Bolt for using a Beam to propagate tuples.
 * @param beamFactory a factory for creating the beam we will use
 * @param queueSize maximum number of tuples to keep in the beam queue
 */
class BeamBolt[EventType](beamFactory: BeamFactory[EventType], batchSize: Int, queueSize: Int, emitMillis: Long)
  extends BaseRichBolt with Logging
{
  def this(beamFactory: BeamFactory[EventType]) = this(beamFactory, 200, 10000, 5000)

  @volatile private var packetizer: BeamPacketizer[Tuple, EventType] = null
  @volatile private var lock      : AnyRef                           = null

  override def prepare(conf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    require(packetizer == null && lock == null, "WTF?! Already initialized, but prepare was called anyway.")
    lock = new AnyRef
    val beam = beamFactory.makeBeam(conf, context)
    val listener = new BeamPacketizerListener[Tuple] {
      override def ack(a: Tuple) = collector.ack(a)

      override def fail(a: Tuple) = collector.fail(a)
    }
    packetizer = new BeamPacketizer[Tuple, EventType](
      beam,
      t => t.getValue(0).asInstanceOf[EventType],
      listener,
      batchSize,
      queueSize,
      emitMillis
    )
    packetizer.start()
  }

  override def execute(tuple: Tuple) {
    packetizer.send(tuple)
  }

  override def cleanup() {
    packetizer.stop()
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
