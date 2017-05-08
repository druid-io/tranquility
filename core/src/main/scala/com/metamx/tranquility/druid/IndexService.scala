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

import com.github.nscala_time.time.Imports._
import com.metamx.common.Backoff
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import com.metamx.common.scala.exception._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.druid.IndexService.TaskHostPort
import com.metamx.tranquility.druid.IndexService.TaskId
import com.metamx.tranquility.finagle._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.Service
import com.twitter.finagle.http
import com.twitter.io.Buf
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import java.net.InetSocketAddress

class IndexService(
  environment: DruidEnvironment,
  config: IndexServiceConfig,
  overlordLocator: OverlordLocator
) extends Closable
{
  private implicit val timer: Timer = DefaultTimer.twitter

  @volatile private var closed: Boolean = false

  @volatile private var _client: Service[http.Request, http.Response] = null

  private def client: Service[http.Request, http.Response] = {
    this.synchronized {
      if (closed) {
        throw new IllegalStateException("Service is closed")
      }

      if (_client == null) {
        _client = overlordLocator.connect()
      }

      _client
    }
  }

  def key: String = environment.indexServiceKey

  def submit(taskBytes: Array[Byte]): Future[TaskId] = {
    val taskRequest = HttpPost("/druid/indexer/v1/task") withEffect {
      req =>
        req.headerMap("Content-Type") = "application/json"
        req.headerMap("Content-Length") = taskBytes.length.toString
        req.content = Buf.ByteArray.Shared(taskBytes)
    }
    log.info(
      "Creating druid indexing task (service = %s): %s",
      environment.indexServiceKey,
      Jackson.pretty(Jackson.parse[Dict](taskBytes))
    )
    call(taskRequest) map {
      d =>
        str(dict(d)("task"))
    } foreach {
      taskId =>
        log.info("Created druid indexing task with id: %s (service = %s)", taskId, environment.indexServiceKey)
    }
  }

  def status(taskId: TaskId): Future[IndexStatus] = {
    val statusRequest = HttpGet("/druid/indexer/v1/task/%s/status" format taskId)
    call(statusRequest) map {
      d =>
        dict(d).get("status") map (sd => IndexStatus.fromString(str(dict(sd)("status")))) getOrElse TaskNotFound
    }
  }

  def runningTasks(): Future[Map[TaskId, TaskHostPort]] = {
    val request = HttpGet("/druid/indexer/v1/runningTasks")
    call(request) map {
      xs =>
        (list(xs).map(dict(_)) map { d =>
          val taskId = str(d("id"))
          val taskLocation = Option(d.getOrElse("location", null)).map(dict(_)).orNull
          val taskHostPort = TaskHostPort.fromMap(taskLocation)
          taskId -> taskHostPort
        }).toMap
    } foreach { tasks =>
      for ((taskId, hostPort) <- tasks) {
        log.debug(s"Found druid indexing task with id[$taskId] at[$hostPort].")
      }
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    this.synchronized {
      closed = true

      if (_client != null) {
        _client.close(deadline)
      } else {
        Future.Done
      }
    }
  }

  private def call(req: http.Request): Future[Any] = {
    val retryable = IndexService.isTransient(config.indexRetryPeriod)
    FutureRetry.onErrors(Seq(retryable), new Backoff(15000, 2, 60000), new DateTime(0)) {
      client(req) map {
        response =>
          response.statusCode match {
            case code if code / 100 == 2 || code == 404 =>
              // 2xx or 404 generally mean legitimate responses from the index service
              Jackson.parse[Any](response.contentString) mapException {
                case e: Exception => new IndexServicePermanentException(e, "Failed to parse response")
              }

            case code if code / 100 == 3 =>
              // Index service can issue redirects temporarily, and HttpClient won't follow them, so treat
              // them as generic errors that can be retried
              throw new IndexServiceTransientException(
                "Service[%s] temporarily unreachable: %s %s" format
                  (environment.indexServiceKey, code, response.status.reason)
              )

            case code if code / 100 == 5 =>
              // Server-side errors can be retried
              throw new IndexServiceTransientException(
                "Service[%s] call failed with status: %s %s" format
                  (environment.indexServiceKey, code, response.status.reason)
              )

            case code =>
              // All other responses should not be retried (including non-404 client errors)
              log.error("Non-retryable response for uri[%s] with content: %s" format (req.uri, response.contentString))
              throw new IndexServicePermanentException(
                "Service[%s] call failed with status: %s %s" format
                  (environment.indexServiceKey, code, response.status.reason)
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

  case class TaskHostPort(host: String, port: Int)
  {
    def isBound = host != null && host.nonEmpty && port > 0

    def toAddr: Addr = {
      if (!isBound) {
        Addr.Neg
      } else {
        Addr.Bound(Address(new InetSocketAddress(host, port)))
      }
    }
  }

  object TaskHostPort
  {
    def unknown = TaskHostPort("", -1)

    def fromMap(d: Dict): TaskHostPort = {
      if (d == null) {
        unknown
      } else {
        val hostOption = Option(d.getOrElse("host", null)).map(str(_))
        val portOption = Option(d.getOrElse("port", null)).map(int(_))

        (hostOption, portOption) match {
          case (Some(host), Some(port)) if host.nonEmpty && port > 0 => TaskHostPort(host, port)
          case _ => unknown
        }
      }
    }
  }

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
