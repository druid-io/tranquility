package com.metamx.tranquility.test.traits

import backtype.storm.LocalCluster
import backtype.storm.generated.KillOptions
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import com.simple.simplespec.Spec
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._

trait StormRequiringSpec
{
  self: Spec =>

  def withLocalStorm[A](f: (LocalCluster => A)): A = {
    val storm = StormRequiringSpec.sharedCluster
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

object StormRequiringSpec
{
  private lazy val sharedCluster: LocalCluster = new LocalCluster()
}
