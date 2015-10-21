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

package com.metamx.tranquility.druid

import com.google.common.base.Charsets
import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.untyped.str
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.druid.TaskClient.timer
import com.metamx.tranquility.finagle.FutureRetry
import com.metamx.tranquility.finagle._
import com.twitter.finagle.Service
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import java.io.IOException
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.scala_tools.time.Imports._

/**
 * Client for a single Druid task.
 */
class TaskClient(
  val task: TaskPointer,
  client: Service[HttpRequest, HttpResponse],
  dataSource: String,
  quietPeriod: Period,
  retryPeriod: Period,
  indexService: IndexService,
  emitter: ServiceEmitter
) extends Logging with Closable
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

  def apply(request: HttpRequest): Future[Option[HttpResponse]] = {
    val retryable = IndexService.isTransient(retryPeriod)
    FutureRetry.onErrors(Seq(retryable), Backoff.standard(), DateTime.now + quietPeriod) {
      client(request) map {
        response =>
          val code = response.getStatus.getCode
          val reason = response.getStatus.getReasonPhrase
          if (code / 100 == 2) {
            if (log.isTraceEnabled) {
              log.trace(
                "Sent request to task[%s], serviceKey[%s], got response: %s",
                task.id,
                task.serviceKey,
                response.getContent.toString(Charsets.UTF_8)
              )
            }
            Some(response)
          } else {
            throw new IOException(
              "Failed to send request to task[%s]: %s %s" format(task.id, code, reason)
            )
          }
      } rescue {
        case e: Exception if retryable(e) =>
          // This is a retryable exception.
          (if (!active) {
            // Possibly give up by returning None if the task has completed.
            Future(status)
          } else {
            indexService.status(task.id)
          }) map {
            newStatus =>
              status = newStatus
              if (!active) {
                // Task inactive, let's give up
                emitAlert(
                  log, emitter, WARN, "Loss of Druid redundancy: %s" format dataSource, Dict(
                    "dataSource" -> dataSource,
                    "task" -> task.id,
                    "status" -> client.status.toString
                  )
                )
                None
              } else {
                // Task still active, allow retry
                throw new IOException("Failed to send request to task[%s] (status = %s)" format(task.id, status), e)
              }
          }
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    client.close(deadline)
  }
}

object TaskClient
{
  implicit val timer: Timer = DefaultTimer.twitter
}

case class TaskPointer(id: String, serviceKey: String)
{
  def toMap = Dict(
    "id" -> id,
    "serviceKey" -> serviceKey
  )
}

object TaskPointer
{
  def fromMap(d: Dict): TaskPointer = {
    TaskPointer(
      str(d("id")),
      str(d("serviceKey"))
    )
  }
}
