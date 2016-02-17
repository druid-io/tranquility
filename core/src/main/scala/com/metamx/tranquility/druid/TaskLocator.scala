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
import com.metamx.tranquility.finagle.DruidTaskResolver
import com.metamx.tranquility.finagle.FinagleRegistry
import com.twitter.finagle.Service
import com.twitter.finagle.http

trait TaskLocator
{
  def maybeAddResolvers(disco: () => Disco, druidTaskResolver: () => DruidTaskResolver): Unit

  def connect(taskPointer: TaskPointer): Service[http.Request, http.Response]
}

class CuratorTaskLocator(
  finagleRegistry: FinagleRegistry,
  druidEnvironment: DruidEnvironment
) extends TaskLocator
{
  override def maybeAddResolvers(disco: () => Disco, druidTaskResolver: () => DruidTaskResolver): Unit = {
    if (!finagleRegistry.schemes.contains("disco")) {
      finagleRegistry.addResolver(new DiscoResolver(disco()))
    }
  }

  override def connect(taskPointer: TaskPointer): Service[http.Request, http.Response] =
  {
    val service = druidEnvironment.firehoseServicePattern format taskPointer.serviceKey
    finagleRegistry.connect("disco", service)
  }
}

class OverlordTaskLocator(
  finagleRegistry: FinagleRegistry,
  druidEnvironment: DruidEnvironment
) extends TaskLocator
{
  override def maybeAddResolvers(disco: () => Disco, druidTaskResolver: () => DruidTaskResolver): Unit = {
    if (!finagleRegistry.schemes.contains("disco")) {
      finagleRegistry.addResolver(new DiscoResolver(disco()))
    }

    val dtrScheme = DruidTaskResolver.mkscheme(druidEnvironment.indexServiceKey)

    if (!finagleRegistry.schemes.contains(dtrScheme)) {
      val resolver = druidTaskResolver()
      require(
        resolver.scheme == dtrScheme,
        s"WTF?! DruidTaskResolver scheme[${resolver.scheme}] did not match expected scheme[$dtrScheme]"
      )
      finagleRegistry.addResolver(resolver)
    }
  }

  override def connect(taskPointer: TaskPointer): Service[http.Request, http.Response] =
  {
    finagleRegistry.connect(DruidTaskResolver.mkscheme(druidEnvironment.indexServiceKey), taskPointer.id)
  }
}

object TaskLocator
{
  val Curator  = "curator"
  val Overlord = "overlord"

  private[tranquility] def create(
    kind: String,
    finagleRegistry: FinagleRegistry,
    druidEnvironment: DruidEnvironment
  ): TaskLocator =
  {
    kind match {
      case `Curator` => new CuratorTaskLocator(finagleRegistry, druidEnvironment)
      case `Overlord` => new OverlordTaskLocator(finagleRegistry, druidEnvironment)
      case _ => throw new IllegalArgumentException(s"Locator[$kind] is not a recognized kind of TaskLocator.")
    }
  }
}
