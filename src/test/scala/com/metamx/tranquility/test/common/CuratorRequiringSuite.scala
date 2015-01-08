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
