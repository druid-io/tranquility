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

package com.metamx.tranquility.beam

import com.metamx.common.scala.Logging
import com.twitter.util.Await
import com.twitter.util.Future
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Wraps a Beam and exposes a single-message API rather than the future-batch-based API. Not thread-safe.
 *
 * @param beam The wrapped Beam.
 * @param listener Handler for successfully and unsuccessfully sent messages. Will be called from the same thread
 *                 that you use to call 'send' or 'flush'.
 * @param batchSize Send a batch after receiving this many messages. Set to 1 to send messages as soon as they arrive.
 * @param maxPendingBatches Maximum number of batches that may be in flight before we block and wait for one to finish.
 */
@deprecated("use Tranquilizer or SimpleTranquilizerAdapter", "0.7.0")
class BeamPacketizer[A](
  beam: Beam[A],
  listener: BeamPacketizerListener[A],
  batchSize: Int,
  maxPendingBatches: Int
) extends Logging
{
  require(maxPendingBatches >= 1, "maxPendingBatches >= 1")

  var started       : Boolean                               = false
  var buffer        : mutable.Buffer[A]                     = new ArrayBuffer[A]()
  val pendingBatches: mutable.Buffer[(Seq[A], Future[Int])] = new java.util.LinkedList[(Seq[A], Future[Int])]().asScala

  def start() {
    started = true
  }

  /**
   * Send a single message. May block if maxPendingBatches has been reached.
   */
  def send(message: A) {
    requireStarted()
    buffer += message
    if (buffer.size >= batchSize) {
      swap()
      if (pendingBatches.size >= maxPendingBatches) {
        awaitPendingBatches(1)
      }
    }
  }

  def flush() = {
    requireStarted()
    if (buffer.nonEmpty) {
      swap()
    }
    if (pendingBatches.nonEmpty) {
      awaitPendingBatches(pendingBatches.size)
    }
  }

  def close() {
    flush()
    started = false
    Await.result(beam.close())
  }

  private def requireStarted() {
    if (!started) {
      throw new IllegalStateException("Not started")
    }
  }

  private def swap() {
    pendingBatches += ((buffer, beam.propagate(buffer)))
    buffer = new ArrayBuffer[A]()
  }

  private def awaitPendingBatches(count: Int) {
    require(count > 0, "count > 0")
    val batches = new ArrayBuffer[(Seq[A], Future[Int])](count)
    for (i <- 0 until count) {
      batches += pendingBatches.remove(0)
    }

    val batchResultsFuture: Future[Seq[(Option[Exception], Int, Seq[A])]] = Future.collect(
      batches map {
        case (batch, future) =>
          future map {
            i =>
              (None, i, batch)
          } handle {
            case e: Exception =>
              (Some(e), 0, batch)
          }
      }
    )

    val batchResults = Await.result(batchResultsFuture)
    for ((eOption, actuallySent, batch) <- batchResults) {
      eOption match {
        case Some(e) =>
          log.warn("%s: Failed to send %,d messages.", beam, batch.size)
          batch.foreach(listener.fail(e, _))

        case None =>
          log.debug("%s: Flushed %,d, ignored %,d messages.", beam, actuallySent, batch.size - actuallySent)
          batch.foreach(listener.ack)
      }
    }
  }
}

trait BeamPacketizerListener[A]
{
  def ack(a: A)

  def fail(e: Throwable, a: A)
}
