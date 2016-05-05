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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.metamx.common.parsers.ParseException
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.Walker
import com.metamx.common.scala.untyped._
import io.druid.data.input.InputRow
import io.druid.data.input.impl.StringInputRowParser
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import scala.collection.JavaConverters._

object Messages
{
  val DataSourceFields = Seq("dataSource", "feed")

  def fromStringStream(
    in: InputStream,
    parser: StringInputRowParser
  ): Walker[InputRow] =
  {
    new Walker[InputRow]
    {
      override def foreach(f: InputRow => Unit): Unit = {
        var lineNumber: Long = 1L
        try {
          new BufferedReader(new InputStreamReader(in, Charsets.UTF_8)).withFinally(_.close()) { reader =>
            val iter = Iterator.continually(reader.readLine()).takeWhile(_ != null)
            for (string <- iter) {
              f(parser.parse(string))
              lineNumber += 1
            }
          }
        }
        catch {
          case e: ParseException =>
            throw new HttpException(HttpResponseStatus.BAD_REQUEST, s"Malformed string on line[$lineNumber]")
        }
      }
    }
  }

  def fromObjectStream(
    in: InputStream,
    forceDataSource: Option[String],
    objectMapper: ObjectMapper
  ): Walker[(String, Dict)] =
  {
    new Walker[(String, Dict)]
    {
      override def foreach(f: ((String, Dict)) => Unit): Unit = {
        try {
          objectMapper.getFactory.createParser(in).withFinally(_.close()) { jp =>
            // Skip past initial array token, if present.
            jp.nextToken()
            if (jp.getCurrentToken == JsonToken.START_ARRAY) {
              jp.nextToken()
            }

            // Read values off the stream.
            objectMapper.readValues(jp, classOf[Dict]).withFinally(_.close()) { jiter =>
              val iter = jiter.asScala
              iter foreach { d =>
                val dataSource = forceDataSource getOrElse {
                  DataSourceFields collectFirst {
                    case field if d contains field => str(d(field))
                  } getOrElse {
                    throw new HttpException(
                      HttpResponseStatus.BAD_REQUEST,
                      s"Missing ${DataSourceFields.map("'" + _ + "'").mkString(" or ")}"
                    )
                  }
                }

                f((dataSource, d))
              }
            }
          }
        }
        catch {
          case e: JsonProcessingException =>
            throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Malformed JSON")
        }
      }
    }
  }
}
