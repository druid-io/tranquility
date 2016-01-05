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

package com.metamx.tranquility.example

import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.CountAggregatorFactory
import io.druid.query.aggregation.LongSumAggregatorFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.joda.time.DateTime
import org.joda.time.Period

object ScalaExample extends Logging
{
  def main(args: Array[String]) {
    // Your overlord's druid.service.
    val indexService = "overlord"

    // Your overlord's druid.discovery.curator.path.
    val discoveryPath = "/druid/discovery"

    // Your Druid schema.
    val dataSource = "foo"
    val dimensions = Seq("bar", "qux")
    val aggregators = Seq(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("baz", "baz"))

    // Tranquility needs to be able to extract timestamps from your object type (in this case, Map[String, Any]).
    val timestamper = (message: Map[String, Any]) => new DateTime(message("timestamp"))

    // Tranquility uses ZooKeeper (through Curator) for coordination.
    val curator = CuratorFrameworkFactory
      .builder()
      .connectString("zk.example.com:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
      .build()
    curator.start()

    // Build a Tranquilizer that we can use to send messages.
    val druidService = DruidBeams
      .builder(timestamper)
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation.create(indexService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      )
      .buildTranquilizer()

    try {
      // Build a sample message to send; make sure we use a current date
      val obj = Map("timestamp" -> new DateTime().toString, "bar" -> "barVal", "baz" -> 3)

      // Send messages to Druid:
      val future: Future[Unit] = druidService.send(obj)

      // Wait for confirmation:
      Await.result(future)
    }
    catch {
      case e: Exception =>
        log.warn(e, "Failed to send message")
    }
    finally {
      druidService.stop()
      curator.close()
    }
  }
}
