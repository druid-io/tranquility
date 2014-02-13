package com.metamx.starfire.tranquility.beam

import com.google.common.base.Charsets
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import com.metamx.common.scala.event.WARN
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.net.uri._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.starfire.tranquility.finagle._
import com.metamx.starfire.tranquility.typeclass.{ObjectWriter, Timestamper}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{InetResolver, Group}
import com.twitter.util.{Future, Timer}
import java.io.{ByteArrayOutputStream, IOException}
import java.util.zip.GZIPOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpRequest
import org.joda.time.{Duration, Period}
import org.scala_tools.time.Implicits._
import org.apache.commons.codec.binary.Base64

/**
 * Emits events over http.
 *
 * This class is a little bit half-baked and might not work.
 *
 * @param uri service uri
 * @param auth basic authentication token (username:password, non-base64ed)
 */
class HttpBeam[A: Timestamper](
  uri: URI,
  auth: Option[String],
  eventWriter: ObjectWriter[A],
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
    val preTlsClientBuilder = ClientBuilder()
      .name(uri.toString)
      .codec(Http())
      .group(Group.fromVarAddr(InetResolver.bind(hostAndPort)))
      .hostConnectionLimit(2)
      .hostConnectionMaxLifeTime(HttpBeam.DefaultConnectionMaxLifeTime)
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

  private[this] def request(events: Seq[A]): HttpRequest = HttpPost(uri.path) withEffect {
    req =>
      val bytes = (new ByteArrayOutputStream withEffect {
        baos =>
          val gzos = new GZIPOutputStream(baos)
          for (event <- events) {
            gzos.write(eventWriter.asBytes(event))
            gzos.write('\n')
          }
          gzos.close()
      }).toByteArray
      req.headers.set("Host", hostAndPort)
      req.headers.set("Content-Type", "text/plain")
      req.headers.set("Content-Encoding", "gzip")
      req.headers.set("Content-Length", bytes.size)
      for (x <- auth) {
        req.headers.set("Authorization", "Basic %s" format Base64.encodeBase64(x.getBytes(Charsets.UTF_8)))
      }
      req.setContent(ChannelBuffers.wrappedBuffer(bytes))
  }

  private[this] def isTransient(period: Period): Exception => Boolean = {
    (e: Exception) => Seq(
      ifException[java.io.IOException],
      ifException[com.twitter.finagle.RequestException],
      ifException[com.twitter.finagle.ChannelException],
      ifException[com.twitter.finagle.TimeoutException],
      ifException[org.jboss.netty.channel.ChannelException],
      ifException[org.jboss.netty.channel.ConnectTimeoutException],
      ifException[org.jboss.netty.handler.timeout.TimeoutException]
    ).exists(_ apply e)
  } untilPeriod period

  def propagate(events: Seq[A]) = {
    val responses = events.grouped(HttpBeam.DefaultBatchSize) map {
      eventsChunk =>
        val retryable = isTransient(HttpBeam.DefaultRetryPeriod)
        val response = {
          client(request(eventsChunk)) map {
            response =>
              response.getStatus.getCode match {
                case code if code / 100 == 2 =>
                  // 2xx means our events were accepted
                  eventsChunk.size

                case code =>
                  throw new IOException(
                    "Service call to %s failed with status: %s %s" format
                      (uri, code, response.getStatus.getReasonPhrase)
                  )
              }
          } retryWhen retryable
        }
        response rescue {
          case e: Exception =>
            // Alert, drop
            emitAlert(
              e, log, emitter, WARN, "Failed to send events: %s" format uri, Map(
                "eventCount" -> events.size
              )
            )
            Future.value(0)
        }
    }
    Future.collect(responses.toSeq).map(_.sum)
  }

  def close() = client.close()

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
