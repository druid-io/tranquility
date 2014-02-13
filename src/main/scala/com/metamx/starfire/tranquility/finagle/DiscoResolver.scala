package com.metamx.starfire.tranquility.finagle

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
