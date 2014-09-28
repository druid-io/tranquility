package com.metamx.tranquility.beam

import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent._
import com.twitter.util.Await
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.collection.mutable.ArrayBuffer

/**
 * Wraps a Beam and exposes a single-message API rather than the future-batch-based API. Internally uses a queue and
 * single thread for sending data.
 *
 * @param batchSize Send a batch after receiving this many messages. Set to 1 to send messages as soon as they arrive.
 * @param queueSize Maximum number of messages to keep in the beam queue.
 * @param emitMillis Send a batch after this much elapsed time. Set to 0 or negative to disable time-based sending.
 */
class BeamPacketizer[A, B](
  beam: Beam[B],
  converter: A => B,
  listener: BeamPacketizerListener[A],
  batchSize: Int,
  queueSize: Int,
  emitMillis: Long
) extends Logging
{
  require(batchSize <= queueSize, "batchSize <= queueSize")

  sealed trait QueueItem
  case class MessageItem(message: A) extends QueueItem
  case object FlushItem extends QueueItem

  // Used by all threads to synchronize access to "unflushedCount", and by the emitThread to signal that all messages
  // may have been flushed.
  private val flushCondition = new AnyRef
  private var unflushedMessageCount = 0

  // Messages that need to be beamed out.
  private val queue = new ArrayBlockingQueue[QueueItem](queueSize)

  private val emitThread = new Thread(
    abortingRunnable {
      try {
        var startMillis = System.currentTimeMillis()
        val batch = new ArrayBuffer[A](batchSize)
        val itemIterator = Iterator.continually {
          if (emitMillis <= 0) {
            Some(queue.take())
          } else {
            val waitMillis = startMillis + emitMillis - System.currentTimeMillis()
            if (waitMillis > 0) {
              // queue.poll returns null when timed out
              Option(queue.poll(waitMillis, TimeUnit.MILLISECONDS))
            } else {
              None
            }
          }
        }
        for (item <- itemIterator) {
          if (Thread.currentThread().isInterrupted) {
            throw new InterruptedException
          }

          val shouldFlush = item match {
            case Some(MessageItem(m)) =>
              // Side effect: insert message into current batch.
              batch += m
              batch.size >= batchSize

            case None | Some(FlushItem) =>
              // Timeout, or flush command. Either way, we should flush.
              true
          }

          // Send a batch on timeout (null message) or full batch
          if (shouldFlush) {
            startMillis = System.currentTimeMillis()

            // Drain the remainder of the queue, up to batchSize.
            for (item <- Iterator.continually(queue.poll()).take(batchSize - batch.size).takeWhile(_ != null)) {
              item match {
                case MessageItem(m) =>
                  batch += m
                case FlushItem =>
                  // Do nothing. We're already flushing.
              }
            }

            if (batch.nonEmpty) {
              try {
                log.debug("Sending %,d queued messages.", batch.size)
                val sent = Await.result(beam.propagate(batch.map(converter)))
                log.debug("Sent %,d, ignored %,d queued messages.", sent, batch.size - sent)
                batch foreach listener.ack
              }
              catch {
                case e: Exception =>
                  log.warn(e, "Failed to send %,d queued messages.", batch.size)
                  batch foreach listener.fail
              }

              // Whether the messages succeeded or failed, they have been flushed.
              flushCondition.synchronized {
                unflushedMessageCount -= batch.size
                flushCondition.notifyAll()
              }

              batch.clear()
            }
          }
        }
      }
      catch {
        case e: InterruptedException =>
        // Exit quietly when interrupted; abort on other throwables.
      }
    }
  )

  def start() {
    log.info("Starting send thread for: %s" format beam)

    emitThread.setName("BeamPacketizer[%s]" format beam)
    emitThread.setDaemon(true)
    emitThread.start()
  }

  def stop() {
    flush()
    emitThread.interrupt()
    Await.result(beam.close())
  }

  def send(a: A) {
    flushCondition.synchronized {
      unflushedMessageCount = unflushedMessageCount + 1
    }
    queue.put(MessageItem(a))
  }

  def flush() {
    queue.put(FlushItem)
    flushCondition.synchronized {
      while (unflushedMessageCount != 0) {
        flushCondition.wait()
      }
    }
  }
}

trait BeamPacketizerListener[A]
{
  def ack(a: A)

  def fail(a: A)
}
