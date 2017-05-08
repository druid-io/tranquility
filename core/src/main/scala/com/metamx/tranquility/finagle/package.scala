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
package com.metamx.tranquility

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Predef._
import com.twitter.finagle.http
import org.joda.time.Duration
import scala.language.implicitConversions

package object finagle
{
  val TwitterDuration = com.twitter.util.Duration
  type TwitterDuration = com.twitter.util.Duration

  implicit def jodaDurationToTwitterDuration(duration: Duration): TwitterDuration = {
    TwitterDuration.fromMilliseconds(duration.millis)
  }

  lazy val FinagleLogger = java.util.logging.Logger.getLogger("finagle")

  def HttpPost(path: String): http.Request = {
    http.Request(http.Method.Post, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  def HttpGet(path: String): http.Request = {
    http.Request(http.Method.Get, path) withEffect {
      req =>
        decorateRequest(req)
    }
  }

  private[this] def decorateRequest(req: http.Request): Unit = {
    // finagle-http doesn't set the host header, and we don't actually know what server we're hitting
    req.headerMap("Host") = "127.0.0.1"
    req.headerMap("Accept") = "*/*"
  }
}
