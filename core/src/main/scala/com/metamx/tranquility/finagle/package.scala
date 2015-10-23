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
package com.metamx.tranquility

import com.metamx.common.scala.Predef._
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.handler.codec.http.HttpMethod
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpVersion
import org.joda.time.Duration
import org.scala_tools.time.Implicits._
import scala.language.implicitConversions

package object finagle
{
  val TwitterDuration = com.twitter.util.Duration
  type TwitterDuration = com.twitter.util.Duration

  implicit def jodaDurationToTwitterDuration(duration: Duration): TwitterDuration = {
    TwitterDuration.fromMilliseconds(duration.millis)
  }

  lazy val FinagleLogger = java.util.logging.Logger.getLogger("finagle")

  def HttpPost(path: String): DefaultHttpRequest = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  def HttpGet(path: String): DefaultHttpRequest = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  private[this] def decorateRequest(req: HttpRequest): HttpHeaders = {
    // finagle-http doesn't set the host header, and we don't actually know what server we're hitting
    req.headers.set("Host", "127.0.0.1")
    req.headers.set("Accept", "*/*")
  }
}
