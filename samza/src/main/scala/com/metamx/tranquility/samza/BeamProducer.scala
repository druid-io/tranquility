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
package com.metamx.tranquility.samza

import com.metamx.common.scala.Logging
import com.metamx.tranquility.tranquilizer.SimpleTranquilizerAdapter
import com.metamx.tranquility.tranquilizer.Tranquilizer
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
  // stream => sender
  private val senders = mutable.Map[String, SimpleTranquilizerAdapter[Any]]()

  override def start() {}

  override def stop() {
    for (sender <- senders.values) {
      sender.stop()
    }
  }

  override def register(source: String) {}

  override def send(source: String, envelope: OutgoingMessageEnvelope) {
    val streamName = envelope.getSystemStream.getStream
    val message = envelope.getMessage
    val sender = senders.getOrElseUpdate(
      streamName, {
        log.info("Creating beam for stream[%s.%s].", systemName, streamName)
        val t = Tranquilizer.create(
          beamFactory.makeBeam(new SystemStream(systemName, streamName), config),
          batchSize,
          maxPendingBatches,
          Tranquilizer.DefaultLingerMillis
        )
        t.start()
        t.simple(false)
      }
    )

    try {
      sender.send(message)
    }
    catch {
      case e: Exception =>
        log.error(e, "Send failed")
        if (throwOnError) {
          throw e
        }
    }
  }

  override def flush(source: String) {
    // So flippin' lazy. Flush ALL the data!
    for ((streamName, sender) <- senders) {
      try {
        sender.flush()
      }
      catch {
        case e: Exception =>
          log.error(e, "Send failed")
          if (throwOnError) {
            throw e
          }
      }
    }
  }
}
