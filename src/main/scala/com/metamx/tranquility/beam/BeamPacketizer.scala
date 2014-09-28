package com.metamx.tranquility.beam

import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent._
import com.twitter.util.Await
import java.util.concurrent.ArrayBlockingQueue
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * Wraps a Beam and exposes a single-message API rather than the future-batch-based API. Internally uses a queue and
 * single thread for sending data.
 *
 * @param queueSize maximum number of messages to keep in the beam queue
 */
class BeamPacketizer[A, B](
  beam: Beam[B],
  converter: A => B,
  listener: BeamPacketizerListener[A],
  queueSize: Int
) extends Logging
{
  // Used by all threads to synchronize access to "unflushedCount", and by the emitThread to signal that all messages
  // may have been flushed.
  private val flushCondition = new AnyRef
  private var unflushedCount = 0
  private val queue          = new ArrayBlockingQueue[A](queueSize)

  private val emitThread = new Thread(
    abortingRunnable {
      try {
        while (!Thread.currentThread().isInterrupted) {
          // Drain at least one element from the queue.
          val emittableJava = new ju.ArrayList[A]()
          emittableJava.add(queue.take())
          queue.drainTo(emittableJava)

          // Emit all drained messages.
          val emittableScala = emittableJava.asScala
          if (emittableScala.nonEmpty) {
            try {
              log.info("Sending %,d queued messages.", emittableScala.size)
              val sent = Await.result(beam.propagate(emittableScala.map(converter)))
              log.info("Sent %,d, ignored %,d queued messages.", sent, emittableScala.size - sent)
              emittableScala foreach listener.ack
            }
            catch {
              case e: Exception =>
                log.warn(e, "Failed to send %,d queued messages.", emittableScala.size)
                emittableScala foreach listener.fail
            }

            // Whether the messages succeeded or failed, they have been flushed.
            flushCondition.synchronized {
              unflushedCount -= emittableScala.size
              flushCondition.notifyAll()
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
      unflushedCount = unflushedCount + 1
    }
    queue.put(a)
  }

  def flush() {
    flushCondition.synchronized {
      while (unflushedCount != 0) {
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
