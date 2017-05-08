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
package com.metamx.tranquility.beam

import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.metamx.common.Backoff
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.net.finagle.InetAddressResolver
import com.metamx.common.scala.net.uri._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.finagle._
import com.metamx.tranquility.typeclass.ObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.finagle.Name
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.http.Request
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Timer
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.GZIPOutputStream

/**
  * Emits messages over http.
  *
  * This class is a little bit half-baked and might not work.
  *
  * @param uri  service uri
  * @param auth basic authentication token (username:password, non-base64ed)
  */
class HttpBeam[A: Timestamper](
  uri: URI,
  auth: Option[String],
  objectWriter: ObjectWriter[A],
  emitter: ServiceEmitter
) extends Beam[A] with Logging
{
  private[this] implicit val timer: Timer = DefaultTimer.twitter

  private[this] val port = if (uri.port > 0) {
    uri.port
  } else if (uri.scheme == "https") {
    443
  } else {
    80
  }

  private[this] val hostAndPort = "%s:%s" format(uri.host, port)

  private[this] val client = {
    val resolver = InetAddressResolver.default
    val preTlsClientBuilder = ClientBuilder()
      .name(uri.toString)
      .codec(Http())
      .dest(Name.Bound(resolver.bind(hostAndPort), "%s!%s" format(resolver.scheme, hostAndPort)))
      .hostConnectionLimit(2)
      .configured(ExpiringService.Param(util.Duration.Top, HttpBeam.DefaultConnectionMaxLifeTime))
      .tcpConnectTimeout(HttpBeam.DefaultConnectTimeout)
      .timeout(HttpBeam.DefaultTimeout)
      .logger(FinagleLogger)
      .daemon(true)
    if (uri.scheme == "https") {
      preTlsClientBuilder.tls(uri.host).build()
    } else {
      preTlsClientBuilder.build()
    }
  }

  private[this] def request(messages: Seq[A]): Request = HttpPost(uri.path) withEffect {
    req =>
      val bytes = (new ByteArrayOutputStream withEffect {
        baos =>
          val gzos = new GZIPOutputStream(baos)
          for (message <- messages) {
            gzos.write(objectWriter.asBytes(message))
            gzos.write('\n')
          }
          gzos.close()
      }).toByteArray
      req.headerMap("Host") = hostAndPort
      req.headerMap("Content-Type") = "text/plain"
      req.headerMap("Content-Encoding") = "gzip"
      req.headerMap("Content-Length") = bytes.size.toString
      for (x <- auth) {
        val base64 = BaseEncoding.base64().encode(x.getBytes(Charsets.UTF_8))
        req.headerMap("Authorization") = "Basic %s" format base64
      }
      req.content = Buf.ByteArray.Owned(bytes)
  }

  private[this] def isTransient(period: Period): Exception => Boolean = {
    (e: Exception) => Seq(
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

  override def sendAll(messages: Seq[A]): Seq[Future[SendResult]] = {
    val messagesWithPromises = Vector() ++ messages.map(message => (message, Promise[SendResult]()))
    for (chunk <- messagesWithPromises.grouped(HttpBeam.DefaultBatchSize)) {
      val retryable = isTransient(HttpBeam.DefaultRetryPeriod)
      val response: Future[SendResult] = FutureRetry.onErrors(Seq(retryable), Backoff.standard(), new DateTime(0)) {
        client(request(chunk.map(_._1))) map {
          response =>
            response.statusCode match {
              case code if code / 100 == 2 =>
                // 2xx means our messages were accepted
                SendResult.Sent

              case code =>
                throw new IOException(
                  "Service call to %s failed with status: %s %s" format
                    (uri, code, response.status.reason)
                )
            }
        }
      } handle {
        case e: Exception =>
          // Alert, drop
          emitAlert(
            e, log, emitter, WARN, "Failed to send messages: %s" format uri, Map(
              "messageCount" -> messages.size
            )
          )
          SendResult.Dropped
      }

      // All messages in the chunk have the same response
      for ((message, promise) <- chunk) {
        promise.become(response)
      }
    }
    messagesWithPromises.map(_._2)
  }

  override def close() = client.close()

  override def toString = "HttpBeam(%s)" format uri
}

object HttpBeam
{
  val DefaultConnectTimeout: Duration = 5.seconds.standardDuration

  val DefaultConnectionMaxLifeTime: Duration = 5.minutes.standardDuration

  val DefaultTimeout: Duration = 30.seconds.standardDuration

  val DefaultRetryPeriod: Period = 10.minutes

  val DefaultBatchSize: Int = 500
}
