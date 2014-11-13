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

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent.abortingThread
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.BeamPacketizer
import com.metamx.tranquility.beam.BeamPacketizerListener
import java.{util => ju}

/**
 * A Storm Bolt for using a Beam to propagate tuples.
 *
 * @param beamFactory Factory for creating the Beam we will use.
 * @param batchSize Send a batch after receiving this many messages. Set to 1 to send messages as soon as they arrive.
 * @param queueSize Maximum number of pending messages allowed at any one time.
 * @param emitMillis Force a flush this often.
 */
class BeamBolt[EventType](
  beamFactory: BeamFactory[EventType],
  batchSize: Int,
  queueSize: Int,
  emitMillis: Long
) extends BaseRichBolt with Logging
{
  def this(beamFactory: BeamFactory[EventType]) = this(beamFactory, 1000, 10000, 5000)

  @volatile private var running    : Boolean               = false
  @volatile private var lock       : AnyRef                = null
  @volatile private var flushThread: Thread                = null
  @volatile private var packetizer : BeamPacketizer[Tuple] = null

  override def prepare(conf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    require(lock == null, "WTF?! Already initialized, but prepare was called anyway.")
    lock = new AnyRef
    lock.synchronized {
      val beam = beamFactory.makeBeam(conf, context)
      flushThread = abortingThread {
        try {
          while (!Thread.currentThread().isInterrupted && running) {
            lock.synchronized {
              if (running) {
                packetizer.flush()
              }
            }
            Thread.sleep(emitMillis)
          }
        } catch {
          case e: InterruptedException => // Exit peacefully
        }
      }
      flushThread.setDaemon(true)
      flushThread.setName("FlushThread-%s" format beam)
      val tupleBeam = new Beam[Tuple] {
        override def propagate(events: Seq[Tuple]) = beam.propagate(events.map(_.getValue(0).asInstanceOf[EventType]))
        override def close() = beam.close()
      }
      val listener = new BeamPacketizerListener[Tuple] {
        override def ack(a: Tuple) = collector.ack(a)
        override def fail(e: Throwable, a: Tuple) = collector.fail(a)
      }
      packetizer = new BeamPacketizer[Tuple](
        tupleBeam,
        listener,
        batchSize,
        if (queueSize > batchSize) queueSize / batchSize else 1
      )
      running = true
      packetizer.start()
      flushThread.start()
    }
  }

  override def execute(tuple: Tuple) {
    lock.synchronized {
      packetizer.send(tuple)
    }
  }

  override def cleanup() {
    lock.synchronized {
      packetizer.close()
      running = false
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
