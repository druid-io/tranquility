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
package com.metamx.tranquility.druid

import com.google.common.base.Charsets
import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.DefunctBeamException
import com.metamx.tranquility.finagle._
import com.metamx.tranquility.typeclass.ObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.finagle.Service
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Future
import java.io.IOException
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.scala_tools.time.Imports._

/**
 * A Beam that writes all events to a fixed set of Druid RealtimeIndexTasks. All events are sent to all tasks.
 */
class DruidBeam[A : Timestamper](
  private[druid] val interval: Interval,
  private[druid] val partition: Int,
  private[druid] val tasks: Seq[DruidTaskPointer],
  location: DruidLocation,
  config: DruidBeamConfig,
  finagleRegistry: FinagleRegistry,
  indexService: IndexService,
  emitter: ServiceEmitter,
  timekeeper: Timekeeper,
  objectWriter: ObjectWriter[A]
) extends Beam[A] with Logging
{
  private[this] implicit val timer = DefaultTimer.twitter

  // Keeps track of each task's client and most recently checked status
  class TaskClient(val task: DruidTaskPointer, val client: Service[HttpRequest, HttpResponse])
  {
    // Assume tasks start running
    @volatile private[this] var _status: IndexStatus = TaskRunning

    def status_=(newStatus: IndexStatus) = synchronized {
      // Don't allow transitions out of inactive states
      if (active && _status != newStatus) {
        log.info("Task %s status changed from %s -> %s", task.id, _status, newStatus)
        _status = newStatus
      }
    }

    def status = _status

    def active = Seq(TaskRunning, TaskNotFound) contains _status

    def apply(x: HttpRequest) = client(x)
  }

  private[this] val clients = Map(
    tasks map {
      task =>
        val service = location.environment.firehoseServicePattern format task.firehoseId
        task -> new TaskClient(task, finagleRegistry.checkout(service))
    }: _*
  )

  def propagate(events: Seq[A]) = {
    val eventsChunks = events
      .grouped(config.firehoseChunkSize)
      .map(xs => (objectWriter.batchAsBytes(xs), xs.size))
      .toList
    // Futures will be the number of events pushed, or an exception. Zero events pushed means we gave up on the task.
    val taskChunkFutures: Seq[Future[(DruidTaskPointer, Int)]] = for {
      (eventsChunk, eventsChunkSize) <- eventsChunks
      task <- tasks
      client <- clients.get(task) if client.active
    } yield {
      val eventPost = HttpPost(
        "/druid/worker/v1/chat/%s/push-events" format
          (location.environment.firehoseServicePattern format task.firehoseId)
      ) withEffect {
        req =>
          req.headers.set("Content-Type", objectWriter.contentType)
          req.headers.set("Content-Length", eventsChunk.length)
          req.setContent(ChannelBuffers.wrappedBuffer(eventsChunk))
      }
      if (log.isTraceEnabled) {
        log.trace(
          "Sending %,d events to task[%s], firehose[%s]: %s",
          eventsChunkSize,
          task.id,
          task.firehoseId,
          new String(eventsChunk)
        )
      }
      val retryable = IndexService.isTransient(config.firehoseRetryPeriod)
      FutureRetry.onErrors(Seq(retryable), Backoff.standard()) {
        client(eventPost) map {
          response =>
            val code = response.getStatus.getCode
            val reason = response.getStatus.getReasonPhrase
            if (code / 100 == 2) {
              log.trace(
                "Sent %,d events to task[%s], firehose[%s], got response: %s",
                eventsChunkSize,
                task.id,
                task.firehoseId,
                response.getContent.toString(Charsets.UTF_8)
              )
              task -> eventsChunkSize
            } else {
              throw new IOException(
                "Failed to send %,d events to task[%s]: %s %s" format(eventsChunkSize, task.id, code, reason)
              )
            }
        } rescue {
          case e: Exception if retryable(e) =>
            // This is a retryable exception. Possibly give up by returning false if the task has disappeared.
            (if (!client.active) {
              Future(client.status)
            } else {
              indexService.status(task.id)
            }) map {
              status =>
                client.status = status
                if (!client.active) {
                  // Task inactive, let's give up
                  emitAlert(
                    log, emitter, WARN, "Loss of Druid redundancy: %s" format location.dataSource, Dict(
                      "task" -> task.id,
                      "status" -> client.status.toString,
                      "remaining" -> clients.values.count(_.active)
                    )
                  )
                  task -> 0
                } else {
                  // Task still active, allow retry
                  throw new IOException(
                    "Unable to push events to task: %s (status = %s)" format(task.id, clients(task).status),
                    e
                  )
                }
            }
        }
      }
    }
    val taskSuccessesFuture: Future[Map[DruidTaskPointer, Int]] = Future.collect(taskChunkFutures) map {
      xs =>
        xs.groupBy(_._1).map {
          case (task, tuples) =>
            task -> tuples.map(_._2).sum
        }
    }
    val overallSuccessFuture: Future[Int] = taskSuccessesFuture map {
      xs =>
        val max = if (xs.isEmpty) 0 else xs.values.max
        max withEffect {
          n =>
            if (n == 0) {
              throw new DefunctBeamException("Tasks are all gone: %s" format tasks.map(_.id).mkString(", "))
            }
        }
    }
    overallSuccessFuture
  }

  def close() = {
    log.info(
      "Closing Druid beam for datasource[%s] interval[%s] (tasks = %s)",
      location.dataSource,
      interval,
      tasks.map(_.id).mkString(", ")
    )
    // Timeout due to https://github.com/twitter/finagle/issues/200
    val closeTimeout = 10.seconds.standardDuration
    val futures = clients.values.toList map (taskClient => taskClient.client.close().within(closeTimeout) handle {
      case e: Exception =>
        log.warn(e, "Unable to close Druid client within %s: %s", closeTimeout, taskClient.task.id)
    })
    Future.collect(futures) map (_ => ())
  }

  override def toString = "DruidBeam(interval = %s, partition = %s, tasks = [%s])" format
    (interval, partition, clients.values.map(t => "%s/%s" format(t.task.id, t.task.firehoseId)).mkString("; "))
}

case class DruidTaskPointer(id: String, firehoseId: String)
