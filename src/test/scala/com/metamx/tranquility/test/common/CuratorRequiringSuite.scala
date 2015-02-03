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

package com.metamx.tranquility.test.common

import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import java.net.BindException
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.test.TestingCluster

trait CuratorRequiringSuite
{
  def withLocalCurator[A](f: CuratorFramework => A): A = {
    withZkCluster {
      connectString =>
        CuratorFrameworkFactory
          .newClient(connectString, new BoundedExponentialBackoffRetry(100, 1000, 5))
          .withFinally(_.close()) {
          curator =>
            curator.start()
            f(curator)
        }
    }
  }

  def withZkCluster[A](f: String => A): A = {
    // TestingCluster has a race where it first selects an available port, then closes it, then binds to it again.
    val cluster = retryOnErrors(ifException[BindException]) {
      new TestingCluster(1).withEffect(_.start())
    }
    try {
      f(cluster.getConnectString)
    } finally {
      cluster.close()
    }
  }
}
