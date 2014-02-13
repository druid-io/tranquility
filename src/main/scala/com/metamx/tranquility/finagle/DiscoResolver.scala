/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.tranquility.finagle

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Logging
import com.metamx.common.scala.net.curator.Disco
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.util.{Future, Time, Closable, Var}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.x.discovery.details.ServiceCacheListener
import scala.collection.JavaConverters._

class DiscoResolver(disco: Disco) extends Resolver with Logging
{
  val scheme = "disco"

  def bind(service: String) = Var.async[Addr](Addr.Pending) {
    updatable =>
      val lifecycle = new Lifecycle
      val serviceCache = disco.cacheFor(service, lifecycle)
      def doUpdate() {
        val newInstances = serviceCache.getInstances.asScala.toSet
        log.info("Updating instances for service[%s] to %s", service, newInstances)
        val newSocketAddresses: Set[SocketAddress] = newInstances map
          (instance => new InetSocketAddress(instance.getAddress, instance.getPort))
        updatable.update(Addr.Bound(newSocketAddresses))
      }
      serviceCache.addListener(
        new ServiceCacheListener
        {
          def cacheChanged() {
            doUpdate()
          }

          def stateChanged(curator: CuratorFramework, state: ConnectionState) {
            doUpdate()
          }
        }
      )
      lifecycle.start()
      try {
        doUpdate()
        new Closable {
          def close(deadline: Time) = Future {
            log.info("No longer monitoring service[%s]", service)
            lifecycle.stop()
          }
        }
      } catch {
        case e: Exception =>
          log.warn(e, "Failed to bind to service[%s]", service)
          lifecycle.stop()
          throw e
      }
  }
}
