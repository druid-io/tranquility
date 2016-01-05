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
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.concurrent.loggingThread
import com.metamx.tranquility.beam.Beam
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer

/**
  * Tranquilizers allow you to provide single messages and get a future for each message reporting success or failure.
  * Tranquilizers provide batching and backpressure, unlike Beams which require you to batch messages on your own and
  * do not provide backpressure.
  *
  * The expected use case of this class is to send individual messages efficiently, possibly from multiple threads,
  * and receive success or failure information for each message. This class is thread-safe.
  *
  * To create Tranquilizers, use [[Tranquilizer$.create]], [[Tranquilizer$.builder]], or
  * [[com.metamx.tranquility.druid.DruidBeams$$Builder#buildTranquilizer]].
  */
class Tranquilizer[MessageType] private(
  beam: Beam[MessageType],
  maxBatchSize: Int,
  maxPendingBatches: Int,
  lingerMillis: Long
) extends Service[MessageType, Unit] with Logging
{
  require(maxBatchSize >= 1, "batchSize >= 1")
  require(maxPendingBatches >= 1, "maxPendingBatches >= 1")
  require(lingerMillis >= 0, "lingerMillis >= 0")

  @volatile private var started: Boolean = false

  case class MessageHolder(message: MessageType, future: Promise[Unit])

  // lock synchronizes access to buffer, bufferStartMillis, pendingBatches.
  // lock should be notified when waiters might be waiting: buffer becomes non-empty, or pendingBatches decreases
  private val lock: AnyRef = new AnyRef

  private var buffer           : Buffer[MessageHolder] = new ArrayBuffer[MessageHolder]
  private var bufferStartMillis: Long                  = 0L
  private var pendingBatches   : Int                   = 0

  // closed future lets the close method return asynchronously
  private val closed = Promise[Unit]()

  private val sendThread = loggingThread {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val exBuffer = lock.synchronized {
          while (
            buffer.isEmpty ||
              pendingBatches >= maxPendingBatches ||
              (lingerMillis > 0 && System.currentTimeMillis() < bufferStartMillis + lingerMillis)
          ) {
            if (lingerMillis == 0 || bufferStartMillis == 0) {
              lock.wait()
            } else {
              lock.wait(math.max(1, bufferStartMillis + lingerMillis - System.currentTimeMillis()))
            }
          }
          if (log.isDebugEnabled) {
            log.debug(s"Buffer with ${buffer.size} messages has lingered too long, sending.")
          }

          swap()
        }

        sendBuffer(exBuffer)
      }
    }
    catch {
      case e: InterruptedException =>
        log.debug("Interrupted, exiting thread.")
    }
    finally {
      closed.setValue(())
    }
  } withEffect { t =>
    t.setDaemon(true)
    t.setName(s"Tranquilizer-BackgroundSend-[$beam]")
  }

  /**
    * Start the tranquilizer. Must be called before any calls to "send" or "apply".
    */
  @LifecycleStart
  def start(): Unit = {
    started = true
    sendThread.start()
  }

  /**
    * Same as [[Tranquilizer.send]].
    * @param message the message to send
    * @return a future that resolves when the message is sent
    */
  override def apply(message: MessageType): Future[Unit] = {
    send(message)
  }

  /**
    * Sends a message. Generally asynchronous, but will block to provide backpressure if the internal buffer is full.
    *
    * The future may contain a Unit, in which case the message was successfully sent. Or it may contain an exception,
    * in which case the message may or may not have been successfully sent. One specific exception to look out for is
    * MessageDroppedException, which means that a message was dropped due to unrecoverable reasons. With Druid this
    * can be caused by message timestamps being outside the configured windowPeriod.
    *
    * NB: This can actually be wrong: if *any* message in a batch of messages is dropped, then all messages in that
    * batch will be marked as dropped (even if they actually made it out). This is a limitation of how Tranquility
    * tracks which messages were and were not dropped. This could be improved once
    * https://github.com/druid-io/tranquility/issues/56 is addressed.
    *
    * @param message the message to send
    * @return a future that resolves when the message is sent
    */
  def send(message: MessageType): Future[Unit] = {
    requireStarted()
    if (log.isTraceEnabled) {
      log.trace(s"Sending message: $message")
    }
    val holder = MessageHolder(message, Promise())

    val (exBuffer, future) = lock.synchronized {
      while (buffer.size >= maxBatchSize - 1 && pendingBatches >= maxPendingBatches) {
        if (log.isDebugEnabled) {
          log.debug(
            s"Buffer size[${buffer.size}] >= maxBatchSize[$maxBatchSize] - 1 and " +
              s"pendingBatches[$pendingBatches] >= maxPendingBatches[$maxPendingBatches], waiting..."
          )
        }
        lock.wait()
      }

      if (buffer.isEmpty) {
        bufferStartMillis = System.currentTimeMillis()
        lock.notifyAll()
      }

      buffer += holder

      val _exBuffer: Option[Buffer[MessageHolder]] = if (buffer.size == maxBatchSize) {
        Some(swap())
      } else if (lingerMillis == 0 && pendingBatches < maxPendingBatches) {
        Some(swap())
      } else {
        None
      }

      (_exBuffer, holder.future)
    }

    exBuffer foreach sendBuffer

    future
  }

  /**
    * Create a SimpleTranquilizerAdapter based on this Tranquilizer. You can create multiple adapters based on the
    * same Tranquilizer, and even use them simultaneously.
    *
    * @param reportDropsAsExceptions true if the adapter should report message drops as exceptions. Otherwise, drops
    *                                are ignored (but will still be reflected in the counters).
    * @return a simple adapter
    */
  def simple(reportDropsAsExceptions: Boolean = false): SimpleTranquilizerAdapter[MessageType] = {
    SimpleTranquilizerAdapter.wrap(this, reportDropsAsExceptions)
  }

  /**
    * Stop the tranquilizer, and close the underlying Beam.
    */
  override def close(deadline: Time): Future[Unit] = {
    started = false
    sendThread.interrupt()
    closed flatMap { _ => beam.close() }
  }

  /**
    * Stop the tranquilizer, and close the underlying Beam. Blocks until closed. Generally you should either
    * use this method or [[Tranquilizer.close]], but not both.
    */
  @LifecycleStop
  def stop(): Unit = {
    Await.result(close())
  }

  override def toString = s"Tranquilizer($beam)"

  private def requireStarted() {
    if (!started) {
      throw new IllegalStateException("Not started")
    }
  }

  // Swap out the current buffer immediately, returning it and incrementing pendingBatches.
  // Must be called while holding the "lock".
  // Preconditions: buffer must be non-empty and pendingBatches must be lower than maxPendingBatches.
  private def swap(): Buffer[MessageHolder] = {
    assert(buffer.nonEmpty && buffer.size <= maxBatchSize)
    assert(pendingBatches < maxPendingBatches)

    val _buffer = buffer
    buffer = new ArrayBuffer[MessageHolder]()
    bufferStartMillis = 0L
    pendingBatches += 1
    lock.notifyAll()
    if (log.isDebugEnabled) {
      log.debug(s"Swapping out buffer with ${_buffer.size} messages, $pendingBatches batches now pending.")
    }
    _buffer
  }

  // Send a buffer of messages. Decrement pendingBatches once the send finishes.
  // Generally should be called outside of the "lock".
  private def sendBuffer(myBuffer: Buffer[MessageHolder]): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Sending buffer with ${myBuffer.size} messages.")
    }

    val propagate = try {
      beam.propagate(myBuffer.map(_.message))
    }
    catch {
      case e: Exception =>
        // Should not happen- exceptions should be in the Future. This is a defensive check.
        Future.exception(new IllegalStateException("Propagate call failed", e))
    }

    propagate respond { result =>
      result match {
        case Return(n) if n == myBuffer.size =>
          log.debug(s"Sent $n out of ${myBuffer.size} messages. Marking batch complete.")
          myBuffer.foreach(_.future.setValue(()))

        case Return(n) =>
          log.debug(s"Sent $n out of ${myBuffer.size} messages. Marking batch dropped.")
          val e = new MessageDroppedException(s"Batch not complete ($n/${myBuffer.size})")
          myBuffer.foreach(_.future.setException(e))

        case Throw(e) =>
          log.warn(e, s"Failed to send ${myBuffer.size} messages.")
          myBuffer.foreach(_.future.setException(e))
      }

      lock.synchronized {
        pendingBatches -= 1
        lock.notifyAll()
        if (log.isDebugEnabled) {
          log.debug(s"Sent buffer, $pendingBatches batches pending.")
        }
      }
    }
  }
}

/**
  * Exception indicating that a message was dropped "on purpose" by the beam. This is not a recoverable exception
  * and so the message must be discarded.
  */
class MessageDroppedException(message: String) extends Exception(message)

object Tranquilizer
{
  val DefaultMaxBatchSize      = 2000
  val DefaultMaxPendingBatches = 5
  val DefaultLingerMillis      = 0

  def apply[MessageType](
    beam: Beam[MessageType],
    maxBatchSize: Int = DefaultMaxBatchSize,
    maxPendingBatches: Int = DefaultMaxPendingBatches,
    lingerMillis: Long = DefaultLingerMillis
  ): Tranquilizer[MessageType] =
  {
    new Tranquilizer[MessageType](beam, maxBatchSize, maxPendingBatches, lingerMillis)
  }

  /**
    * Wraps a Beam and exposes a single-message-future API. Thread-safe.
    *
    * @param beam The wrapped Beam.
    */
  def create[MessageType](
    beam: Beam[MessageType]
  ): Tranquilizer[MessageType] =
  {
    new Tranquilizer[MessageType](
      beam,
      DefaultMaxBatchSize,
      DefaultMaxPendingBatches,
      DefaultLingerMillis
    )
  }

  /**
    * Wraps a Beam and exposes a single-message-future API. Thread-safe.
    *
    * @param beam The wrapped Beam.
    * @param maxBatchSize Maximum number of messages to send at once.
    * @param maxPendingBatches Maximum number of batches that may be in flight before we block and wait for one to finish.
    * @param lingerMillis Wait this long for batches to collect more messages (up to maxBatchSize) before sending them.
    *                     Set to zero to disable waiting.
    */
  def create[MessageType](
    beam: Beam[MessageType],
    maxBatchSize: Int,
    maxPendingBatches: Int,
    lingerMillis: Long
  ): Tranquilizer[MessageType] =
  {
    new Tranquilizer[MessageType](beam, maxBatchSize, maxPendingBatches, lingerMillis)
  }

  /**
    * Returns a builder for creating Tranquilizer instances.
    */
  def builder(): Builder = {
    new Builder(Config())
  }

  private case class Config(
    maxBatchSize: Int = DefaultMaxBatchSize,
    maxPendingBatches: Int = DefaultMaxPendingBatches,
    lingerMillis: Long = DefaultLingerMillis
  )

  class Builder private[tranquilizer](config: Config)
  {
    /**
      * Maximum number of messages to send at once. Optional, default is 5000.
      * @param n max batch size
      * @return new builder
      */
    def maxBatchSize(n: Int) = {
      new Builder(config.copy(maxBatchSize = n))
    }

    /**
      * Maximum number of batches that may be in flight before we block and wait for one to finish. Optional, default
      * is 5.
      * @param n max pending batches
      * @return new builder
      */
    def maxPendingBatches(n: Int) = {
      new Builder(config.copy(maxPendingBatches = n))
    }

    /**
      * Wait this long for batches to collect more messages (up to maxBatchSize) before sending them. Set to zero to
      * disable waiting. Optional, default is zero.
      * @param n linger millis
      * @return new builder
      */
    def lingerMillis(n: Long) = {
      new Builder(config.copy(lingerMillis = n))
    }

    /**
      * Build a Tranquilizer.
      * @param beam beam to wrap
      * @return tranquilizer
      */
    def build[MessageType](beam: Beam[MessageType]): Tranquilizer[MessageType] = {
      new Tranquilizer[MessageType](beam, config.maxBatchSize, config.maxPendingBatches, config.lingerMillis)
    }
  }

}
