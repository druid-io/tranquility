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
package com.metamx.tranquility.finagle

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.net.finagle.DiscoResolver
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.Set
import scala.collection.mutable

/**
  * Registry of shared Finagle services.
  */
class FinagleRegistry(
  config: FinagleRegistryConfig,
  resolvers: Seq[Resolver]
) extends Logging
{
  // Backwards compatible constructor.
  def this(config: FinagleRegistryConfig, disco: Disco) = {
    this(config, Seq(new DiscoResolver(disco)))
  }

  private val lock        = new AnyRef
  private val clients     = mutable.HashMap[String, SharedService[http.Request, http.Response]]()
  private val resolverMap = mutable.HashMap[String, Resolver]()

  // Add all resolvers from the constructor.
  resolvers foreach addResolver

  def addResolver(resolver: Resolver): Unit = {
    lock.synchronized {
      if (resolverMap.contains(resolver.scheme)) {
        throw new IllegalArgumentException(s"Already have resolver for scheme[${resolver.scheme}]")
      }

      log.info(s"Adding resolver for scheme[${resolver.scheme}].")
      resolverMap.put(resolver.scheme, resolver)
    }
  }

  def schemes: Set[String] = lock.synchronized {
    resolverMap.keySet
  }

  private def mkid(scheme: String, name: String): String = s"$scheme!$name"

  private def mkclient(scheme: String, name: String): SharedService[http.Request, http.Response] =
  {
    val id = mkid(scheme, name)
    val resolver = synchronized {
      resolverMap.getOrElse(
        scheme, {
          throw new IllegalStateException(s"No resolver for scheme[$scheme], cannot resolve service[$id]")
        }
      )
    }
    val client = ClientBuilder()
      .name(id)
      .codec(http.Http())
      .dest(Name.Bound(resolver.bind(name), id))
      .hostConnectionLimit(config.finagleHttpConnectionsPerHost)
      .timeout(config.finagleHttpTimeout.standardDuration)
      .logger(FinagleLogger)
      .daemon(true)
      .failFast(config.finagleEnableFailFast)
      .build()
    new SharedService(
      new ServiceProxy(client)
      {
        override def close(deadline: Time) = {
          // Called when the SharedService decides it is done
          lock.synchronized {
            log.info("Closing client for service: %s", id)
            clients.remove(id)
          }
          try {
            super.close(deadline)
          }
          catch {
            case e: Exception =>
              log.warn(e, "Failed to close client for service: %s", id)
              Future.Done
          }
        }
      }
    ) withEffect {
      _ =>
        log.info("Created client for service: %s", id)
    }
  }

  def connect(scheme: String, name: String): Service[http.Request, http.Response] = {
    val id = mkid(scheme, name)
    val client = lock.synchronized {
      clients.get(id) match {
        case Some(c) =>
          c.incrementRefcount()
          c

        case None =>
          val c = mkclient(scheme, name)
          clients.put(id, c)
          c
      }
    }
    val closed = new AtomicBoolean(false)
    new ServiceProxy(client)
    {
      override def close(deadline: Time) = {
        // Called when the checked-out client wants to be returned
        if (closed.compareAndSet(false, true)) {
          client.close(deadline)
        } else {
          log.warn(s"WTF?! Service[$id] closed more than once by the same checkout.")
          Future.Done
        }
      }
    }
  }
}
