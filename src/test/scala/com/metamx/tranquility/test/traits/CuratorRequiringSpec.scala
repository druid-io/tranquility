package com.metamx.tranquility.test.traits

import com.simple.simplespec.Spec
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.test.TestingCluster

trait CuratorRequiringSpec
{
  self: Spec =>

  def withLocalCurator[A](f: CuratorFramework => A): A = {
    val cluster = new TestingCluster(1)
    val curator = CuratorFrameworkFactory.newClient(
      cluster.getConnectString,
      new BoundedExponentialBackoffRetry(100, 1000, 5)
    )
    cluster.start()
    curator.start()
    try f(curator)
    finally {
      curator.close()
      cluster.close()
    }
  }
}
