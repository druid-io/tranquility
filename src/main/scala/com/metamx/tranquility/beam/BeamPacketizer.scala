package com.metamx.tranquility.beam

import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent._
import com.twitter.util.Await
import java.util.concurrent.ArrayBlockingQueue
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * Wraps a Beam and exposes a single-event API rather than the future-batch-based API. Internally uses a queue and
 * single thread for sending data.
 *
 * @param queueSize maximum number of tuples to keep in the beam queue
 * @param emitMillis emit at least this often
 */
class BeamPacketizer[A, B](
  beam: Beam[B],
  converter: A => B,
  listener: BeamPacketizerListener[A],
  queueSize: Int,
  emitMillis: Long
) extends Logging
{
  // Used by the send(a) to signal that the queue is full and a flush is needed ASAP.
  private val flushNeededCondition = new AnyRef

  // Used by the emitThread to signal that the queue may have been flushed.
  private val possiblyFlushedCondition = new AnyRef

  private val queue = new ArrayBlockingQueue[A](queueSize)

  private val emitThread = new Thread(
    abortingRunnable {
      try {
        while (!Thread.currentThread().isInterrupted) {
          val startMillis = System.currentTimeMillis()
          val emittableJava = new ju.ArrayList[A]()
          queue.drainTo(emittableJava)

          val emittableScala = emittableJava.asScala
          if (emittableScala.nonEmpty) {
            try {
              log.info("Sending %,d queued events.", emittableScala.size)
              val sent = Await.result(beam.propagate(emittableScala.map(converter)))
              log.info("Sent %,d, ignored %,d queued events.", sent, emittableScala.size - sent)
              emittableScala foreach listener.ack
              possiblyFlushedCondition.synchronized {
                possiblyFlushedCondition.notifyAll()
              }
            }
            catch {
              case e: Exception =>
                log.warn(e, "Failed to send %,d queued events.", emittableScala.size)
                emittableScala foreach listener.fail
            }
          }
          val waitMillis = startMillis + emitMillis - System.currentTimeMillis()
          if (waitMillis > 0) {
            flushNeededCondition.synchronized {
              flushNeededCondition.wait(waitMillis)
            }
          }
        }
      } catch {
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
    if (!queue.offer(a)) {
      flushNeededCondition.synchronized {
        flushNeededCondition.notifyAll()
      }
      queue.put(a)
    }
  }

  def flush() {
    flushNeededCondition.synchronized {
      flushNeededCondition.notifyAll()
    }
    possiblyFlushedCondition.synchronized {
      while (!queue.isEmpty) {
        possiblyFlushedCondition.wait()
      }
    }
  }
}

trait BeamPacketizerListener[A]
{
  def ack(a: A)

  def fail(a: A)
}
