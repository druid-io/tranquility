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
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.Walker
import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.untyped._
import java.io.Closeable
import java.io.InputStream
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import scala.collection.JavaConverters._
import sun.reflect.annotation.ExceptionProxy

object Messages
{
  val DataSourceFields = Seq("dataSource", "feed")

  def fromInputStreamV1(
    objectMapper: ObjectMapper,
    in: InputStream,
    forceDataSource: Option[String]
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
