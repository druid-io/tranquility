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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.hash.Hashing
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.metamx.tranquility.partition.MapPartitioner
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import io.druid.query.aggregation.DoubleSumAggregatorFactory
import java.{util => ju}

import io.druid.java.util.common.granularity.Granularities
import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.scalatest.Matchers

class MapPartitionerTest extends FunSuite with Matchers
{
  val same = Seq(
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 1, "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 1, "bar" -> Seq("z", "y", "")),
    Dict("t" -> new DateTime("2000T00:00:04"), "foo" -> Seq(1), "bar" -> Seq("z", "y"), "baz" -> Nil),
    Dict("t" -> new DateTime("2000T00:00:05"), "foo" -> 1, "bar" -> Seq("y", "z"), "baz" -> ""),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> 1, "bar" -> Seq("y", "z"), "baz" -> null),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> "1", "bar" -> Seq("y", "z"), "baz" -> null),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> Seq("z", "y"))
  )

  val different = Seq(
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 1, "bar" -> Seq("y", "z"), "baz" -> "buh"),
    Dict("t" -> new DateTime("2000T00:01:03"), "foo" -> 1, "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:03"), "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 2, "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> "3", "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> "y"),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> "z"),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> Seq("y", "z", "x")),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> Seq("z", "x"))
  )

  test("Scala Map") {
    val timestamper = new Timestamper[Dict] {
      override def timestamp(a: Dict): DateTime = new DateTime(a("t"))
    }
    val partitioner = MapPartitioner.create(
      timestamper,
      new TimestampSpec("t", "auto", null),
      DruidRollup(
        SpecificDruidDimensions(Seq("foo", "bar", "baz")),
        Seq(new DoubleSumAggregatorFactory("x", "xSum")),
        Granularities.MINUTE,
        true
      )
    )

    for (x <- same; y <- same) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)
      xPartition should be(yPartition)
    }

    for (x <- same; y <- different) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }

    for ((x, i) <- different.zipWithIndex; y <- different.drop(i + 1)) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }
  }

  test("Java Map") {
    val timestamper = new Timestamper[ju.Map[String, Any]] {
      override def timestamp(a: ju.Map[String, Any]): DateTime = new DateTime(a.get("t"))
    }
    val partitioner = MapPartitioner.create(
      timestamper,
      new TimestampSpec("t", "auto", null),
      DruidRollup(
        SpecificDruidDimensions(Seq("foo", "bar", "baz")),
        Seq(new DoubleSumAggregatorFactory("x", "xSum")),
        Granularities.MINUTE,
        true
      )
    )

    val sameJava: Seq[ju.Map[String, Any]] = same map javaCopy
    val differentJava: Seq[ju.Map[String, Any]] = different map javaCopy

    for (x <- sameJava; y <- sameJava) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)
      xPartition should be(yPartition)
    }

    for (x <- sameJava; y <- differentJava) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }

    for ((x, i) <- differentJava.zipWithIndex; y <- differentJava.drop(i + 1)) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }
  }

  test("String") {
    val timestamper = new Timestamper[String] {
      override def timestamp(a: String): DateTime = new DateTime(1000)
    }
    val partitioner = MapPartitioner.create(
      timestamper,
      new TimestampSpec("t", "auto", null),
      DruidRollup(
        SpecificDruidDimensions(Seq("foo", "bar", "baz")),
        Seq(new DoubleSumAggregatorFactory("x", "xSum")),
        Granularities.MINUTE,
        true
      )
    )

    partitioner.partition("foo", Int.MaxValue) should be(Hashing.consistentHash("foo".hashCode, Int.MaxValue))
  }

  private def javaCopy(d: Dict): ju.Map[String, Any] = {
    new ObjectMapper().readValue(Jackson.bytes(d), classOf[ju.Map[String, Any]])
  }
}
