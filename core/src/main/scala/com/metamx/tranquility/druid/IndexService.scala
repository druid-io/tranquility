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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.metamx.common.Backoff
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import com.metamx.common.scala.exception._
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.druid.IndexService.TaskId
import com.metamx.tranquility.finagle._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Timer
import io.druid.indexing.common.task.Task
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpRequest
import org.scala_tools.time.Imports._

class IndexService(
  environment: DruidEnvironment,
  config: IndexServiceConfig,
  finagleRegistry: FinagleRegistry,
  druidObjectMapper: ObjectMapper,
  lifecycle: Lifecycle
)
{
  private[this] implicit val timer: Timer = DefaultTimer.twitter

  private[this] val client = finagleRegistry.checkout(environment.indexService)

  lifecycle onStop {
    Await.result(client.close())
  }

  def submit(task: Task): Future[TaskId] = {
    val taskJson = druidObjectMapper.writeValueAsBytes(task)
    val taskRequest = HttpPost("/druid/indexer/v1/task") withEffect {
      req =>
        req.headers.set("Content-Type", "application/json")
        req.headers.set("Content-Length", taskJson.length)
        req.setContent(ChannelBuffers.wrappedBuffer(taskJson))
    }
    log.info("Creating druid indexing task with id: %s (service = %s)", task.getId, environment.indexService)
    call(taskRequest) map {
      d =>
        str(d("task"))
    } foreach {
      taskId =>
        log.info("Created druid indexing task with id: %s (service = %s)", taskId, environment.indexService)
    }
  }

  def status(taskId: TaskId): Future[IndexStatus] = {
    val statusRequest = HttpGet("/druid/indexer/v1/task/%s/status" format taskId)
    call(statusRequest) map {
      d =>
        d.get("status") map (sd => IndexStatus.fromString(str(dict(sd)("status")))) getOrElse TaskNotFound
    }
  }

  private def call(req: HttpRequest): Future[Dict] = {
    val retryable = IndexService.isTransient(config.indexRetryPeriod)
    FutureRetry.onErrors(Seq(retryable), new Backoff(15000, 2, 60000), new DateTime(0)) {
      client(req) map {
        response =>
          response.getStatus.getCode match {
            case code if code / 100 == 2 || code == 404 =>
              // 2xx or 404 generally mean legitimate responses from the index service
              Jackson.parse[Dict](response.getContent.toString(Charsets.UTF_8)) mapException {
                case e: Exception => new IndexServicePermanentException(e, "Failed to parse response")
              }

            case code if code / 100 == 3 =>
              // Index service can issue redirects temporarily, and HttpClient won't follow them, so treat
              // them as generic errors that can be retried
              throw new IndexServiceTransientException(
                "Service[%s] temporarily unreachable: %s %s" format
                  (environment.indexService, code, response.getStatus.getReasonPhrase)
              )

            case code if code / 100 == 5 =>
              // Server-side errors can be retried
              throw new IndexServiceTransientException(
                "Service[%s] call failed with status: %s %s" format
                  (environment.indexService, code, response.getStatus.getReasonPhrase)
              )

            case code =>
              // All other responses should not be retried (including non-404 client errors)
              throw new IndexServicePermanentException(
                "Service[%s] call failed with status: %s %s" format
                  (environment.indexService, code, response.getStatus.getReasonPhrase)
              )
          }
      }
    }
  }
}

sealed trait IndexStatus

case object TaskSuccess extends IndexStatus

case object TaskFailed extends IndexStatus

case object TaskRunning extends IndexStatus

case object TaskNotFound extends IndexStatus

object IndexStatus
{
  def fromString(x: String) = x.toLowerCase match {
    case "success" => TaskSuccess
    case "failed" => TaskFailed
    case "running" => TaskRunning
  }
}

object IndexService
{
  type TaskId = String
  type TaskPayload = Dict

  def isTransient(period: Period): Exception => Boolean = {
    (e: Exception) => Seq(
      ifException[IndexServiceTransientException],
      ifException[java.io.IOException],
      ifException[com.twitter.finagle.RequestException],
      ifException[com.twitter.finagle.ChannelException],
      ifException[com.twitter.finagle.TimeoutException],
      ifException[com.twitter.finagle.ServiceException],
      ifException[org.jboss.netty.channel.ChannelException],
      ifException[org.jboss.netty.channel.ConnectTimeoutException],
      ifException[org.jboss.netty.handler.timeout.TimeoutException]
    ).exists(_ apply e)
  } untilPeriod period
}

sealed trait IndexServiceException

/**
 * Exceptions that indicate transient indexing service failures. Can be retried if desired.
 */
class IndexServiceTransientException(t: Throwable, msg: String, params: Any*)
  extends Exception(msg format (params: _*), t) with IndexServiceException
{
  def this(msg: String, params: Any*) = this(null: Throwable, msg, params)
}

/**
 * Exceptions that are permanent in nature, and are useless to retry externally. The assumption is that all other
 * exceptions may be transient.
 */
class IndexServicePermanentException(t: Throwable, msg: String, params: Any*)
  extends Exception(msg format (params: _*), t) with IndexServiceException
{
  def this(msg: String, params: Any*) = this(null: Throwable, msg, params)
}
