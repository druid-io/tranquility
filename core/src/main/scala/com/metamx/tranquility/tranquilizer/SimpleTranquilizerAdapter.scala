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

package com.metamx.tranquility.tranquilizer

import com.metamx.common.lifecycle.LifecycleStart
import com.metamx.common.lifecycle.LifecycleStop
import com.metamx.common.scala.Logging
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
  * Wrap a Tranquilizer, exposing an API that is meant to be easy to use for a single caller that doesn't care
  * exactly what happens with individual messages, but does care about having exceptions and counts reported
  * eventually.
  *
  * Exceptions are reported from time to time when you call "send", and always when you call "flush". Only one
  * exception will be reported at a time. Note that exceptions triggered by "send" may not necessarily be related to
  * the actual message you passed; they could be from a previous message.
  *
  * The expected use case of the simple adapter is that if you get an exception from "send" or "flush", you should
  * stop using the adapter and create a new one. It is not meant to make it possible to associate specific exceptions
  * with specific messages.
  *
  * Calling "start" or "stop" on this adapter will start or stop the underlying Tranquilizer. If you want to start
  * or stop the underlying Tranquilizer yourself, then do not call "start" or "stop" on this adapter.
  *
  * The expected use case of this class is that it is used by a single thread.
  */
class SimpleTranquilizerAdapter[MessageType] private(
  tranquilizer: Tranquilizer[MessageType],
  reportDropsAsExceptions: Boolean
) extends Logging
{
  // failure tracking; need to report failure eventually as we do not return futures from send
  private val exception = new AtomicReference[Throwable]

  // pending message tracking, for flush
  private val pending = new AtomicLong

  // counters for metrics
  private val received = new AtomicLong
  private val sent     = new AtomicLong
  private val failed   = new AtomicLong

  /**
    * Send a message. The actual send may happen asynchronously. If you must know whether or not there was an
    * exception sending it, call "flush", which will flush all pending messages and throw an exception if there
    * was one.
    *
    * This method may throw an exception if there was one, although it is not guaranteed. Furthermore, if this
    * method does throw an exception, it might be for a different message.
    */
  def send(message: MessageType) {
    maybeThrow()
    received.incrementAndGet()
    pending.incrementAndGet()
    tranquilizer.send(message) respond { res =>
      res match {
        case Return(()) =>
          sent.incrementAndGet()

        case Throw(e: MessageDroppedException) =>
          failed.incrementAndGet()
          if (reportDropsAsExceptions) {
            exception.compareAndSet(null, e)
          }

        case Throw(e) =>
          failed.incrementAndGet()
          exception.compareAndSet(null, e)
      }

      if (pending.decrementAndGet() == 0) {
        pending.synchronized {
          pending.notifyAll()
        }
      }
    }
  }

  /**
    * Wait for all pending messages to flush out, and throw an exception if there was one. Blocks until all
    * pending messages are flushed.
    */
  def flush() {
    tranquilizer.flush()

    // Wait for data to flush out.
    pending.synchronized {
      while (pending.get() > 0) {
        pending.wait()
      }
    }

    maybeThrow()
  }

  /**
    * The number of messages that you've sent to this object so far.
    * @return received count
    */
  def receivedCount: Long = received.get()

  /**
    * The number of messages that have successfully been sent through the underlying Tranquilizer.
    * @return sent count
    */
  def sentCount: Long = sent.get()

  /**
    * The number of messages that have failed to send, including drops. This includes drops even if you have disabled
    * `reportDropsAsExceptions`.
    * @return failed count
    */
  def failedCount: Long = failed.get()

  @LifecycleStart
  def start(): Unit = tranquilizer.start()

  @LifecycleStop
  def stop(): Unit = tranquilizer.stop()

  private def maybeThrow(): Unit = {
    val e = exception.getAndSet(null)
    if (e != null) {
      throw e
    }
  }
}

object SimpleTranquilizerAdapter
{
  def wrap[MessageType](
    tranquilizer: Tranquilizer[MessageType]
  ): SimpleTranquilizerAdapter[MessageType] =
  {
    wrap(tranquilizer, false)
  }

  def wrap[MessageType](
    tranquilizer: Tranquilizer[MessageType],
    reportDropsAsExceptions: Boolean
  ): SimpleTranquilizerAdapter[MessageType] =
  {
    new SimpleTranquilizerAdapter[MessageType](tranquilizer, reportDropsAsExceptions)
  }
}
