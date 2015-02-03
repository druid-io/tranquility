/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.twitter.util.Await
import com.twitter.util.Future
import java.{util => ju}
import scala.collection.mutable.ArrayBuffer

/**
 * A Storm Bolt for using a Beam to propagate tuples.
 *
 * @param beamFactory Factory for creating the Beam we will use.
 * @param batchSize Maximum number of events to send per call to Beam.propagate.
 */
class BeamBolt[EventType](
  beamFactory: BeamFactory[EventType],
  batchSize: Int
) extends BaseRichBolt with Logging
{
  def this(beamFactory: BeamFactory[EventType]) = this(beamFactory, 2000)

  @volatile private var lock      : AnyRef             = null
  @volatile private var running   : Boolean            = false
  @volatile private var buffer    : ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
  @volatile private var sendThread: Thread             = null
  @volatile private var beam      : Beam[EventType]    = null

  override def prepare(conf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    require(lock == null, "WTF?! Already initialized, but prepare was called anyway.")
    lock = new AnyRef
    beam = beamFactory.makeBeam(conf, context)
    sendThread = abortingThread {
      try {
        while (!Thread.currentThread().isInterrupted && running) {
          val tuples = lock.synchronized {
            while (buffer.isEmpty) {
              lock.wait()
            }
            val _buffer = buffer
            buffer = new ArrayBuffer[Tuple]()
            _buffer
          }

          val ok: Boolean = try {
            val futures = (for (batch <- tuples.grouped(batchSize)) yield {
              beam.propagate(batch.map(_.getValue(0).asInstanceOf[EventType]))
            }).toList
            val sent: Int = Await.result(Future.collect(futures.toList)).sum
            log.info("%s: Flushed %,d, ignored %,d messages.", beam, sent, tuples.size - sent)
            true
          }
          catch {
            case e: Exception =>
              log.warn(e, "%s: Failed to send %,d messages.", beam, tuples.size)
              false
          }

          if (ok) {
            tuples foreach collector.ack
          } else {
            tuples foreach collector.fail
          }
        }
      }
      catch {
        case e: InterruptedException => // Exit peacefully
      }
    }
    sendThread.setDaemon(true)
    sendThread.setName("BeamSendThread-%s" format beam)
    running = true
    sendThread.start()
  }

  override def execute(tuple: Tuple) {
    lock.synchronized {
      buffer += tuple
      lock.notifyAll()
    }
  }

  override def cleanup() {
    running = false
    sendThread.interrupt()
    Await.result(beam.close())
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
