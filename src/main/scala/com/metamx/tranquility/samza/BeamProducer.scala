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
