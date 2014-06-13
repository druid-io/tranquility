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

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.test.TestingCluster

trait CuratorRequiringSuite
{
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
