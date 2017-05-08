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
import com.metamx.tranquility.beam.SendResult
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.util.control.NoStackTrace

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
  lingerMillis: Long,
  blockOnFull: Boolean
) extends Service[MessageType, Unit] with Logging
{
  require(maxBatchSize >= 1, "batchSize >= 1")
  require(maxPendingBatches >= 1, "maxPendingBatches >= 1")

  @volatile private var started: Boolean = false

  case class MessageAndPromise(message: MessageType, promise: Promise[Unit])

  // lock synchronizes buffer, bufferStartMillis, pendingBatches, currentBatchNumber, currentBatchFuture, flushing.
  // lock should be notified when waiters might be waiting: buffer becomes non-empty, buffer is sent, flush starts.
  private val lock: AnyRef = new AnyRef

  // Current batch of pending messages. Not yet handed off to the Beam.
  private var buffer: Buffer[MessageAndPromise] = new ArrayBuffer[MessageAndPromise]

  // How long "buffer" has been open for
  private var bufferStartMillis: Long = 0L

  // Batches currently pending in the Beam. Does not include the current buffer
  private var pendingBatches: Map[Long, Promise[Unit]] = Map.empty

  // Index that the current batch will have in pendingBatches when it goes in
  private var currentBatchNumber: Long = 0L

  // Future that the current batch will have in pendingBatches when it goes in
  private var currentBatchFuture: Promise[Unit] = Promise()

  // True if there is a flush in progress
  private var flushing: Boolean = false

  // Lets the close method return asynchronously
  private val closed = Promise[Unit]()

  private val sendThread = loggingThread {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val (exIndex, exBuffer) = lock.synchronized {
          while (
            buffer.isEmpty ||
              pendingBatches.size >= maxPendingBatches ||
              (!flushing &&
                ((lingerMillis < 0 && buffer.size < maxBatchSize) // wait for full batches
                  || (lingerMillis > 0 && System.currentTimeMillis() < bufferStartMillis + lingerMillis)))
          ) {
            if (lingerMillis <= 0 || bufferStartMillis == 0) {
              lock.wait()
            } else {
              lock.wait(math.max(1, bufferStartMillis + lingerMillis - System.currentTimeMillis()))
            }
          }
          if (log.isDebugEnabled) {
            log.debug(s"Sending buffer with ${buffer.size} messages (from background send thread).")
          }

          flushing = false
          swap()
        }

        sendBuffer(exIndex, exBuffer)
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
    *
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
    * @param message message to send
    * @return future that resolves when the message is sent, or fails to send
    * @throws BufferFullException if outgoing queue is full and blockOnFull is false.
    */
  def send(message: MessageType): Future[Unit] = {
    requireStarted()
    if (log.isTraceEnabled) {
      log.trace(s"Sending message: $message")
    }
    val messageAndPromise = MessageAndPromise(message, Promise())

    val (exIndexBufferPair, future) = lock.synchronized {
      while (buffer.size >= maxBatchSize - 1 && pendingBatches.size >= maxPendingBatches) {
        if (blockOnFull) {
          if (log.isDebugEnabled) {
            log.debug(
              s"Buffer size[${buffer.size}] >= maxBatchSize[$maxBatchSize] - 1 and " +
                s"pendingBatches[${pendingBatches.size}] >= maxPendingBatches[$maxPendingBatches], waiting..."
            )
          }
          lock.wait()
        } else {
          throw new BufferFullException
        }
      }

      if (buffer.isEmpty) {
        bufferStartMillis = System.currentTimeMillis()
        lock.notifyAll()
      }

      buffer += messageAndPromise

      val _exIndexBufferPair: Option[(Long, Buffer[MessageAndPromise])] = if (buffer.size == maxBatchSize) {
        Some(swap())
      } else if (lingerMillis == 0 && pendingBatches.size < maxPendingBatches) {
        Some(swap())
      } else {
        None
      }

      (_exIndexBufferPair, messageAndPromise.promise)
    }

    exIndexBufferPair.foreach(t => sendBuffer(t._1, t._2))

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
    * Block until all messages that have been passed to "send" before this call to "flush" have been processed
    * and their futures have been resolved.
    *
    * This method will not throw any exceptions, even if some messages failed to send. You need to check the
    * returned futures for that.
    */
  def flush(): Unit = {
    val futures: Seq[Future[Unit]] = lock.synchronized {
      val _futures = Vector.newBuilder[Future[Unit]]
      pendingBatches.values foreach (_futures += _)
      if (buffer.nonEmpty) {
        flushing = true
        lock.notifyAll()
        _futures += currentBatchFuture
      }
      _futures.result()
    }

    if (log.isDebugEnabled) {
      log.debug(s"Flushing ${futures.size} batches.")
    }

    Await.result(Future.collect(futures))
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
  // Preconditions: buffer must be non-empty and pendingBatches.size must be lower than maxPendingBatches.
  // Postconditions: buffer is empty
  private def swap(): (Long, Buffer[MessageAndPromise]) = {
    assert(buffer.nonEmpty && buffer.size <= maxBatchSize)
    assert(pendingBatches.size < maxPendingBatches)

    val _buffer = buffer
    val _currentBatchNumber = currentBatchNumber
    pendingBatches = pendingBatches + (currentBatchNumber -> currentBatchFuture)
    buffer = new ArrayBuffer[MessageAndPromise]()
    currentBatchNumber += 1
    currentBatchFuture = Promise()
    bufferStartMillis = 0L
    lock.notifyAll()
    if (log.isDebugEnabled) {
      log.debug(s"Swapping out buffer with ${_buffer.size} messages, ${pendingBatches.size} batches now pending.")
    }
    (_currentBatchNumber, _buffer)
  }

  // Send a buffer of messages. Decrement pendingBatches once the send finishes.
  // Generally should be called outside of the "lock".
  private def sendBuffer(myIndex: Long, myBuffer: Buffer[MessageAndPromise]): Unit = {
    if (log.isDebugEnabled) {
      log.debug(s"Sending buffer with ${myBuffer.size} messages.")
    }

    val futureResults: Seq[Future[SendResult]] = try {
      beam.sendAll(myBuffer.map(_.message))
    }
    catch {
      case e: Exception =>
        // Should not happen- exceptions should be in the Future. This is a defensive check.
        myBuffer.map(_ => Future.exception(new IllegalStateException("sendAll failed", e)))
    }

    val remaining = new AtomicInteger(futureResults.size)
    val sent = new AtomicInteger()
    val dropped = new AtomicInteger()
    val failed = new AtomicInteger()

    for ((futureResult, index) <- futureResults.zipWithIndex) {
      futureResult respond { tryResult =>
        val promise = myBuffer(index).promise
        tryResult match {
          case Return(result) =>
            if (result.sent) {
              sent.incrementAndGet()
              promise.setValue(())
            } else {
              dropped.incrementAndGet()
              promise.setException(MessageDroppedException.Instance)
            }

          case Throw(e) =>
            failed.incrementAndGet()
            promise.setException(e)
        }

        if (remaining.decrementAndGet() == 0) {
          lock.synchronized {
            val batchFuture: Promise[Unit] = pendingBatches(myIndex)
            pendingBatches = pendingBatches - myIndex
            batchFuture.setValue(())
            if (log.isDebugEnabled) {
              log.debug(
                s"Sent[${sent.get()}], dropped[${dropped.get()}], failed[${failed.get()}] " +
                  s"out of ${myBuffer.size} messages from batch #$myIndex. " +
                  s"${pendingBatches.size} batches still pending."
              )
            }
            lock.notifyAll()
          }
        }
      }
    }
  }
}

/**
  * Exception indicating that a message was dropped "on purpose" by the beam. This is not a recoverable exception
  * and so the message must be discarded.
  */
class MessageDroppedException private() extends Exception("Message dropped") with NoStackTrace

object MessageDroppedException
{
  val Instance = new MessageDroppedException
}

/**
  * Exception indicating that the outgoing buffer was full. Will only be thrown if "blockOnFull" is false.
  */
class BufferFullException extends Exception("Buffer full")

object Tranquilizer
{
  val DefaultMaxBatchSize      = 2000
  val DefaultMaxPendingBatches = 5
  val DefaultLingerMillis      = 0L
  val DefaultBlockOnFull       = true

  def apply[MessageType](
    beam: Beam[MessageType],
    maxBatchSize: Int = DefaultMaxBatchSize,
    maxPendingBatches: Int = DefaultMaxPendingBatches,
    lingerMillis: Long = DefaultLingerMillis,
    blockOnFull: Boolean = DefaultBlockOnFull
  ): Tranquilizer[MessageType] =
  {
    new Tranquilizer[MessageType](beam, maxBatchSize, maxPendingBatches, lingerMillis, blockOnFull)
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
      DefaultLingerMillis,
      DefaultBlockOnFull
    )
  }

  /**
    * Wraps a Beam and exposes a single-message-future API. Thread-safe.
    *
    * Does not support all options; for the full set of options, use a "builder".
    *
    * @param beam              The wrapped Beam.
    * @param maxBatchSize      Maximum number of messages to send at once.
    * @param maxPendingBatches Maximum number of batches that may be in flight before we block and wait for one to finish.
    * @param lingerMillis      Wait this long for batches to collect more messages (up to maxBatchSize) before sending them.
    *                          Set to zero to disable waiting.
    */
  def create[MessageType](
    beam: Beam[MessageType],
    maxBatchSize: Int,
    maxPendingBatches: Int,
    lingerMillis: Long
  ): Tranquilizer[MessageType] =
  {
    new Tranquilizer[MessageType](beam, maxBatchSize, maxPendingBatches, lingerMillis, DefaultBlockOnFull)
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
    lingerMillis: Long = DefaultLingerMillis,
    blockOnFull: Boolean = DefaultBlockOnFull
  )

  class Builder private[tranquilizer](config: Config)
  {
    /**
      * Maximum number of messages to send at once. Optional, default is 5000.
      *
      * @param n max batch size
      * @return new builder
      */
    def maxBatchSize(n: Int) = {
      new Builder(config.copy(maxBatchSize = n))
    }

    /**
      * Maximum number of batches that may be in flight before we block and wait for one to finish. Optional, default
      * is 5.
      *
      * @param n max pending batches
      * @return new builder
      */
    def maxPendingBatches(n: Int) = {
      new Builder(config.copy(maxPendingBatches = n))
    }

    /**
      * Wait this long for batches to collect more messages (up to maxBatchSize) before sending them. Set to zero to
      * disable waiting. Optional, default is zero.
      *
      * @param n linger millis
      * @return new builder
      */
    def lingerMillis(n: Long) = {
      new Builder(config.copy(lingerMillis = n))
    }

    /**
      * Whether "send" will block (true) or throw an exception (false) when called while the outgoing queue is full.
      * Optional, default is true.
      *
      * @param b flag for blocking when full
      * @return new builder
      */
    def blockOnFull(b: Boolean) = {
      new Builder(config.copy(blockOnFull = b))
    }

    /**
      * Build a Tranquilizer.
      *
      * @param beam beam to wrap
      * @return tranquilizer
      */
    def build[MessageType](beam: Beam[MessageType]): Tranquilizer[MessageType] = {
      new Tranquilizer[MessageType](
        beam,
        config.maxBatchSize,
        config.maxPendingBatches,
        config.lingerMillis,
        config.blockOnFull
      )
    }

    override def toString = s"Tranquilizer.Builder($Config)"
  }

}
