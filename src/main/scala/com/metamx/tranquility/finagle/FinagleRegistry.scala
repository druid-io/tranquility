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
package com.metamx.tranquility.finagle

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.curator._
import com.metamx.common.scala.net.finagle.DiscoResolver
import com.twitter.finagle.Name
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceProxy
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.scala_tools.time.Implicits._
import scala.collection.mutable

/**
 * Registry of shared Finagle HTTP Curator-discovered services. The services can be returned by closing them. When
 * the last reference to a service has been returned, the service is closed.
 */
class FinagleRegistry(config: FinagleRegistryConfig, disco: Disco) extends Logging
{
  private[this] val lock     = new AnyRef
  private[this] val resolver = new DiscoResolver(disco)
  private[this] val clients  = mutable.HashMap[String, SharedService[HttpRequest, HttpResponse]]()

  private[this] def mkclient(service: String) = {
    val client = ClientBuilder()
      .name(service)
      .codec(Http())
      .dest(Name.Bound(resolver.bind(service), "%s!%s" format (resolver.scheme, service)))
      .hostConnectionLimit(config.finagleHttpConnectionsPerHost)
      .timeout(config.finagleHttpTimeout.standardDuration)
      .logger(FinagleLogger)
      .daemon(true)
      .failFast(false) // Generally only one server behind each service (there's one service per Druid task)
      .build()
    new SharedService(
      new ServiceProxy(client)
      {
        override def close(deadline: Time) = {
          // Called when the SharedService decides it is done
          lock.synchronized {
            log.info("Closing client for service: %s", service)
            clients.remove(service)
          }
          try {
            super.close(deadline)
          } catch {
            case e: Exception =>
              log.warn(e, "Failed to close client for service: %s", service)
              Future.Done
          }
        }
      }
    ) withEffect {
      _ =>
        log.info("Created client for service: %s", service)
    }
  }

  def checkout(service: String): Service[HttpRequest, HttpResponse] = {
    val client = lock.synchronized {
      clients.get(service) match {
        case Some(c) =>
          c.incrementRefcount()
          c

        case None =>
          val c = mkclient(service)
          clients.put(service, c)
          c
      }
    }
    val closed = new AtomicBoolean(false)
    new ServiceProxy(client) {
      override def close(deadline: Time) = {
        // Called when the checked-out client wants to be returned
        if (closed.compareAndSet(false, true)) {
          client.close(deadline)
        } else {
          log.warn("WTF?! Service closed more than once by the same checkout: %s", service)
          Future.Done
        }
      }
    }
  }
}
