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
package com.metamx.tranquility.flink

import com.metamx.common.scala.Logging
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.atomic.AtomicReference
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * This class provides a sink that can propagate any event type to Druid.
  *
  * @param beamFactory your implementation of [[BeamFactory]].
  * @param reportDropsAsExceptions throws an exception if set to true and a message gets dropped.
  */
class BeamSink[T](beamFactory: BeamFactory[T], reportDropsAsExceptions: Boolean = false)
  extends RichSinkFunction[T] with Logging
{
  var sender: Option[Tranquilizer[T]] = None

  private val exception       = new AtomicReference[Throwable]()
  private val receivedCounter = new LongCounter()
  private val sentCounter     = new LongCounter()
  private val droppedCounter  = new LongCounter()

  override def open(parameters: Configuration) = {
    sender = Some(beamFactory.tranquilizer)
    getRuntimeContext.addAccumulator("Druid: Messages received", receivedCounter)
    getRuntimeContext.addAccumulator("Druid: Messages sent", sentCounter)
    getRuntimeContext.addAccumulator("Druid: Messages dropped", droppedCounter)
  }

  override def invoke(value: T) = {
    receivedCounter.add(1)
    sender.get.send(value) respond {
      case Return(()) => sentCounter.add(1)
      case Throw(e: MessageDroppedException) if reportDropsAsExceptions => exception.compareAndSet(null, e)
      case Throw(e: MessageDroppedException) => droppedCounter.add(1)
      case Throw(e) => exception.compareAndSet(null, e)
    }

    maybeThrow()
  }

  override def close() = {
    sender.get.flush()
    maybeThrow()
  }

  private def maybeThrow() {
    if (exception.get() != null) {
      throw exception.get()
    }
  }
}
