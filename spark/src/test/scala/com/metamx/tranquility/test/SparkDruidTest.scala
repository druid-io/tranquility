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

package com.metamx.tranquility.test

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.spark.BeamRDD._
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.test.common.DruidIntegrationSuite
import com.metamx.tranquility.test.common.JulUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SparkDruidTest
  extends FunSuite with DruidIntegrationSuite with CuratorRequiringSuite with Logging with BeforeAndAfterAll
{
  private var sparkContext: SparkContext     = null
  private var ssc         : StreamingContext = null

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(
      new SparkConf().setMaster("local").setAppName("SparkDruidTest")
    )
    ssc = new StreamingContext(sparkContext, Seconds(3))
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
    if (sparkContext != null) {
      sparkContext.stop()
    }

  }

  JulUtils.routeJulThroughSlf4j()
  test("Spark to Druid") {
    withDruidStack {
      (curator, broker, coordinator, overlord) =>
        val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
        val now = new DateTime().hourOfDay().roundFloorCopy()

        val inputs = DirectDruidTest.generateEvents(now)
        val lines = mutable.Queue[RDD[SimpleEvent]]()
        lines += sparkContext.makeRDD(inputs, 2)
        val dstream = ssc.queueStream(lines)
        dstream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory(zkConnect)))
        ssc.start()

        runTestQueriesAndAssertions(
          broker, new TestingTimekeeper withEffect {
            timekeeper =>
              timekeeper.now = now
          }
        )

        ssc.stop(true, true)
        ssc.awaitTermination()
    }
  }
}

class SimpleEventBeamFactory(zkConnect: String) extends BeamFactory[SimpleEvent]
{
  override def makeBeam: Beam[SimpleEvent] = {
    SimpleEventBeamFactory.instance(zkConnect)
  }
}

object SimpleEventBeamFactory
{
  // zkConnect -> beams
  private val beams = mutable.HashMap[String, Beam[SimpleEvent]]()

  def instance(zkConnect: String) = {
    beams.synchronized {
      val aDifferentCurator = CuratorFrameworkFactory.newClient(
        zkConnect,
        new BoundedExponentialBackoffRetry(100, 1000, 5)
      )
      aDifferentCurator.start()
      val builder = DirectDruidTest.newBuilder(
        aDifferentCurator, new TestingTimekeeper withEffect {
          timekeeper =>
            timekeeper.now = DateTime.now
        }
      )
      builder.buildBeam()
    }
  }
}
