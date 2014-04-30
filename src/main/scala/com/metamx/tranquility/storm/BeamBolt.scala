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
import com.metamx.common.scala.concurrent.loggingRunnable
import com.metamx.tranquility.beam.Beam
import com.twitter.util.Await
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue}
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * A Storm Bolt for using a Beam to propagate tuples.
 * @param beamFactory a factory for creating the beam we will use
 * @param queueSize maximum number of tuples to keep in the beam queue
 * @param emitMillis emit at least this often
 */
class BeamBolt[EventType](beamFactory: BeamFactory[EventType], queueSize: Int, emitMillis: Long)
  extends BaseRichBolt with Logging
{
  def this(beamFactory: BeamFactory[EventType]) = this(beamFactory, 1000, 1000)

  @volatile private var beam      : Beam[EventType]      = null
  @volatile private var emitThread: Thread               = null
  @volatile private var lock      : AnyRef               = null
  @volatile private var queue     : BlockingQueue[Tuple] = null

  override def prepare(conf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    require(beam == null && lock == null && queue == null, "WTF?! Already initialized, but prepare was called anyway.")
    beam = beamFactory.makeBeam(conf, context)
    lock = new AnyRef
    queue = new ArrayBlockingQueue[Tuple](queueSize)
    val emitRunnable = loggingRunnable {
      while (!Thread.currentThread().isInterrupted) {
        val startMillis = System.currentTimeMillis()
        val emittableJava = new ju.ArrayList[Tuple]()
        val emittable = emittableJava.asScala
        queue.drainTo(emittableJava)
        if (emittable.nonEmpty) {
          try {
            val events: IndexedSeq[EventType] = emittable.map(_.getValue(0).asInstanceOf[EventType]).toIndexedSeq
            log.info("Sending %,d queued events.", events.size)
            val sent = Await.result(beam.propagate(events))
            log.info("Sent %,d, ignored %,d queued events.", sent, events.size - sent)
            emittable foreach collector.ack
          }
          catch {
            case e: Exception =>
              log.warn(e, "Failed to send %,d queued events.", emittable.size)
              emittable foreach collector.fail
          }
        }
        val waitMillis = startMillis + emitMillis - System.currentTimeMillis()
        if (waitMillis > 0) {
          lock.synchronized {
            lock.wait(waitMillis)
          }
        }
      }
    }
    emitThread = new Thread(emitRunnable)
    emitThread.setName("BeamBolt-Emitter-%s-%d" format (context.getThisComponentId, context.getThisTaskIndex))
    emitThread.setDaemon(true)
    emitThread.start()
  }

  override def execute(tuple: Tuple) {
    if (!queue.offer(tuple)) {
      lock.synchronized {
        lock.notifyAll()
      }
      queue.put(tuple)
    }
  }

  override def cleanup() {
    Option(emitThread) foreach (_.interrupt())
    Await.result(beam.close())
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
