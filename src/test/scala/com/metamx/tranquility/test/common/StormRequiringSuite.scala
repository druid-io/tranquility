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

import backtype.storm.LocalCluster
import backtype.storm.generated.KillOptions
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._

trait StormRequiringSuite extends Logging
{
  def withLocalStorm[A](f: (LocalCluster => A)): A = {
    val storm = StormRequiringSuite.sharedCluster
    def killTopology(name: String) {
      retryOnErrors(ifException[Exception] untilPeriod 60.seconds) {
        log.info("Killing topology: %s", name)
        storm.killTopologyWithOpts(name, new KillOptions() withEffect (_.set_wait_secs(0)))
      }
    }
    def getTopologies() = {
      retryOnErrors(ifException[Exception] untilPeriod 60.seconds) {
        Option(storm.getClusterInfo) map (_.get_topologies().asScala) getOrElse {
          throw new IllegalStateException("getClusterInfo returned null!")
        }
      }
    }
    def killAllTopologies() = {
      for (topology <- getTopologies()) {
        killTopology(topology.get_name())
      }
      val start = System.currentTimeMillis()
      while (getTopologies().nonEmpty && System.currentTimeMillis() < start + 60000L) {
        log.info("Waiting for topologies to die...")
        Thread.sleep(2000)
      }
      val topologies = getTopologies()
      if (topologies.nonEmpty) {
        throw new IllegalStateException("Topologies remain: %s" format topologies.map(_.get_name()))
      }
    }
    try {
      f(storm)
    }
    finally {
      killAllTopologies()
    }
  }

}

object StormRequiringSuite
{
  private lazy val sharedCluster: LocalCluster = new LocalCluster()
}
