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

import org.jboss.netty.handler.codec.http.HttpResponseStatus

class HttpException(
  cause: Throwable,
  val status: HttpResponseStatus,
  message0: String
) extends Exception(status.toString, cause)
{
  def this(status: HttpResponseStatus) = this(null, status, null)
  def this(status: HttpResponseStatus, message0: String) = this(null, status, message0)

  def message: String = message0 match {
    case null => status.toString
    case x => x
  }
}
