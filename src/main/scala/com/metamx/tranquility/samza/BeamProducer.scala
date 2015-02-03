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
package com.metamx.tranquility.samza

import com.metamx.common.ISE
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.BeamPacketizer
import com.metamx.tranquility.beam.BeamPacketizerListener
import org.apache.samza.config.Config
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemStream
import scala.collection.mutable

class BeamProducer(
  beamFactory: BeamFactory,
  systemName: String,
  config: Config,
  batchSize: Int,
  maxPendingBatches: Int,
  throwOnError: Boolean
) extends SystemProducer with Logging
{
  // stream => beam packetizer
  private val beams = mutable.Map[String, BeamPacketizer[Any]]()

  override def start() {}

  override def stop() {
    for (beamPacketizer <- beams.values) {
      beamPacketizer.close()
    }
  }

  override def register(source: String) {}

  override def send(source: String, envelope: OutgoingMessageEnvelope) {
    val streamName = envelope.getSystemStream.getStream
    val message = envelope.getMessage
    val beamPacketizer = beams.getOrElseUpdate(streamName, {
      log.info("Creating beam for stream[%s.%s].", systemName, streamName)
      val listener = new BeamPacketizerListener[Any] {
        override def ack(a: Any) {}
        override def fail(e: Throwable, message: Any) {
          if (throwOnError) {
            throw new ISE(e, "Failed to send message[%s]." format message)
          }
        }
      }
      val p = new BeamPacketizer(
        beamFactory.makeBeam(new SystemStream(systemName, streamName), config),
        listener,
        batchSize,
        maxPendingBatches
      )
      p.start()
      p
    })
    beamPacketizer.send(message)
  }

  override def flush(source: String) {
    // So flippin' lazy. Flush ALL the data!
    for ((streamName, beamPacketizer) <- beams) {
      beamPacketizer.flush()
    }
  }
}
