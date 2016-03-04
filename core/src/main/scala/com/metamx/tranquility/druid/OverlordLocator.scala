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

import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.net.finagle.DiscoResolver
import com.metamx.tranquility.finagle.FinagleRegistry
import com.twitter.finagle.Resolver
import com.twitter.finagle.Service
import com.twitter.finagle.http
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response

trait OverlordLocator
{
  def maybeAddResolvers(disco: () => Disco): Unit

  def connect(): Service[http.Request, http.Response]
}

class CuratorOverlordLocator(
  finagleRegistry: FinagleRegistry,
  druidEnvironment: DruidEnvironment
) extends OverlordLocator
{
  override def maybeAddResolvers(disco: () => Disco): Unit = {
    if (!finagleRegistry.schemes.contains("disco")) {
      finagleRegistry.addResolver(new DiscoResolver(disco()))
    }
  }

  override def connect(): Service[Request, Response] = {
    finagleRegistry.connect("disco", druidEnvironment.indexServiceKey)
  }
}

object OverlordLocator
{
  val Curator = "curator"

  private[tranquility] def create(
    kind: String,
    finagleRegistry: FinagleRegistry,
    druidEnvironment: DruidEnvironment
  ): OverlordLocator =
  {
    kind match {
      case `Curator` => new CuratorOverlordLocator(finagleRegistry, druidEnvironment)
      case _ => throw new IllegalArgumentException(s"Locator[$kind] is not a recognized kind of OverlordLocator.")
    }
  }
}
