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

package com.metamx.tranquility.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import com.metamx.common.CompressionUtils
import com.metamx.common.scala.Abort
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Walker
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.server.http.TranquilityServlet._
import com.metamx.tranquility.tranquilizer.BufferFullException
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.twitter.util.Return
import com.twitter.util.Throw
import io.druid.data.input.InputRow
import java.io.InputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream
import javax.ws.rs.core.MediaType
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.scalatra.ScalatraServlet
import scala.collection.JavaConverters._
import scala.collection.mutable

class TranquilityServlet(
  dataSourceBundles: Map[String, DataSourceBundle]
) extends ScalatraServlet with Logging
{
  get("/") {
    contentType = "text/plain"
    """
      |
      |
      |
      |                            ╒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▄▄▄▄
      |                             ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█▓▓▓▄
      |                                                          ▀█▓▓╕
      |                                                             █▓▓
      |              ▄▓▓▓▓▓▄    ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄          ▀▓▓
      |              ▀▀▀▀▀▀▀    ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█▓▓▄        ▀▓▓
      |                                                     ▀▓▓µ       ▓▓µ
      |                                                      ▐▓▓       ▓▓▄
      |                                                       ▓▓       ▓▓▄
      |                                                      ╒▓▓       ▓▓
      |                                                     ,▓▓Γ      ▄▓█
      |                                                    ▄▓▓Γ      ╓▓▓
      |                                               ,▄▄▓▓█▀       y▓▓
      |                     ▀▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▀▀         ▓▓█
      |                                                         ,▓▓▓▀
      |                                                      ,▄▓▓█▀
      |                                                ,▄▄▓▓▓▓█▀
      |                             └▓▓▓▓▓▓▓   ▀▓▓▓▓▓▓▓███▀▀
      |
      |
      | """.stripMargin
  }

  post("/v1/post") {
    val async = yesNo("async", false)
    doV1Post(None, async)
  }

  post("/v1/post/:dataSource") {
    val async = yesNo("async", false)
    doV1Post(Some(params("dataSource")), async)
  }

  notFound {
    status = 404
    contentType = "text/plain"
    "Not found\n"
  }

  error {
    case e: HttpException =>
      log.debug(e, s"User error serving request to ${request.uri}")
      status = e.status.getCode
      contentType = "text/plain"
      e.message + "\n"

    case e: Exception =>
      log.warn(e, s"Server error serving request to ${request.uri}")
      status = 500
      contentType = "text/plain"
      "Server error\n"

    case e: Throwable =>
      Abort(e)
  }

  private def doV1Post(forceDataSource: Option[String], async: Boolean): Array[Byte] = {
    val objectMapper = getObjectMapper()
    val decompressor = getRequestDecompressor()
    val messages: Walker[(String, InputRow)] = request.contentType match {
      case Some(JsonContentType) | Some(SmileContentType) =>
        Messages.fromObjectStream(decompressor(request.inputStream), forceDataSource, objectMapper) map {
          case (dataSource, d) =>
            val row = getBundle(dataSource).mapParser.parse(d.asJava.asInstanceOf[java.util.Map[String, AnyRef]])
            (dataSource, row)
        }

      case Some(TextContentType) =>
        val dataSource = forceDataSource getOrElse {
          throw new HttpException(
            HttpResponseStatus.BAD_REQUEST,
            s"Must include dataSource in URL for contentType[$TextContentType]"
          )
        }
        Messages.fromStringStream(decompressor(request.inputStream), getBundle(dataSource).stringParser) map { row =>
          (dataSource, row)
        }

      case _ =>
        throw new HttpException(
          HttpResponseStatus.BAD_REQUEST,
          s"Expected contentType $JsonContentType, $SmileContentType, or $TextContentType"
        )
    }
    val (received, sent) = doSend(messages, async)
    val result = Dict("result" -> Dict("received" -> received, "sent" -> sent))
    contentType = getOutputContentType()
    objectMapper.writeValueAsBytes(result)
  }

  private def getObjectMapper(): ObjectMapper = {
    request.contentType match {
      case Some(SmileContentType) => SmileObjectMapper
      case _ => JsonObjectMapper
    }
  }

  private def getOutputContentType(): String = {
    request.contentType match {
      case Some(SmileContentType) => SmileContentType
      case _ => JsonContentType
    }
  }

  private def getRequestDecompressor(): InputStream => InputStream = {
    request.header("Content-Encoding") match {
      case Some("gzip") | Some("x-gzip") =>
        in => CompressionUtils.gzipInputStream(in)
      case Some("identity") | None =>
        identity
      case Some(x) =>
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Unrecognized request Content-Encoding")
    }
  }

  private def doSend(messages: Walker[(String, InputRow)], async: Boolean): (Long, Long) = {
    val myBundles = mutable.HashMap[String, DataSourceBundle]()
    val received = new AtomicLong
    val sent = new AtomicLong
    val exception = new AtomicReference[Throwable]
    for ((dataSource, message) <- messages) {
      val bundle = myBundles.getOrElseUpdate(dataSource, getBundle(dataSource))

      received.incrementAndGet()

      val future = try {
        bundle.tranquilizer.send(message)
      }
      catch {
        case e: BufferFullException =>
          throw new HttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, s"Buffer full for dataSource '$dataSource'")
      }

      future respond {
        case Return(_) => sent.incrementAndGet()
        case Throw(e: MessageDroppedException) => // Suppress
        case Throw(e) => exception.compareAndSet(null, e)
      }

      // async => ignore sent, exception; just receive things.
      if (!async && exception.get() != null) {
        throw exception.get()
      }
    }

    // async => ignore sent, exception; just receive things.
    if (!async) {
      myBundles.values.foreach(_.tranquilizer.flush())

      if (exception.get() != null) {
        throw exception.get()
      }
    }

    (received.get(), if (async) 0L else sent.get())
  }

  private def getBundle(dataSource: String): DataSourceBundle = {
    dataSourceBundles.getOrElse(
      dataSource, {
        throw new HttpException(HttpResponseStatus.BAD_REQUEST, s"No definition for dataSource '$dataSource'")
      }
    )
  }

  private def yesNo(k: String, defaultValue: Boolean): Boolean = {
    request.parameters.get(k) map { s =>
      s.toLowerCase() match {
        case "true" | "1" | "yes" => true
        case "false" | "0" | "no" | "" => false
        case _ =>
          throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Expected true or false")
      }
    } getOrElse defaultValue
  }

}

object TranquilityServlet
{
  val JsonObjectMapper  = Jackson.newObjectMapper()
  val SmileObjectMapper = Jackson.newObjectMapper(new SmileFactory)

  val JsonContentType  = MediaType.APPLICATION_JSON
  val SmileContentType = SmileMediaTypes.APPLICATION_JACKSON_SMILE
  val TextContentType  = MediaType.TEXT_PLAIN
}
