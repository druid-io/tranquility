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

package com.metamx.tranquility.druid.input

import com.metamx.common.scala.untyped.Dict
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl.DimensionsSpec
import io.druid.data.input.impl.MapInputRowParser
import io.druid.data.input.impl.TimeAndDimsParseSpec
import io.druid.data.input.impl.TimestampSpec
import io.druid.java.util.common.granularity.Granularities
import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

import scala.collection.JavaConverters._

class InputRowPartitionerTest extends FunSuite with ShouldMatchers
{
  val row = new MapBasedInputRow(
    new DateTime("2000"),
    Seq("foo", "baz").asJava,
    Map[String, AnyRef](
      "foo" -> "x",
      "bar" -> Int.box(2),
      "baz" -> "what",
      "hey" -> "there"
    ).asJava
  )

  val parser = new MapInputRowParser(
    new TimeAndDimsParseSpec(
      new TimestampSpec("t", "iso", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Seq("foo", "bar", "baz").asJava), null, null)
    )
  )

  val partitioner = new InputRowPartitioner(Granularities.MINUTE)

  val same = Seq(
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 1, "bar" -> Seq("y", "z")),
    Dict("t" -> new DateTime("2000T00:00:03"), "foo" -> 1, "bar" -> Seq("z", "y", "")),
    Dict("t" -> new DateTime("2000T00:00:04"), "foo" -> Seq(1), "bar" -> Seq("z", "y"), "baz" -> Nil),
    Dict("t" -> new DateTime("2000T00:00:05"), "foo" -> 1, "bar" -> Seq("y", "z"), "baz" -> ""),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> 1, "bar" -> Seq("y", "z"), "baz" -> null),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> "1", "bar" -> Seq("y", "z"), "baz" -> null),
    Dict("t" -> new DateTime("2000T00:00:06"), "foo" -> Seq("1"), "bar" -> Seq("z", "y"))
  ) map (d => parser.parse(d.asJava.asInstanceOf[java.util.Map[String, AnyRef]]))

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
  ) map (d => parser.parse(d.asJava.asInstanceOf[java.util.Map[String, AnyRef]]))

  test("Same vs same") {
    for (x <- same; y <- same) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)
      xPartition should be(yPartition)
    }
  }

  test("Same vs different") {
    for (x <- same; y <- different) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }
  }

  test("Different vs different") {
    for ((x, i) <- different.zipWithIndex; y <- different.drop(i + 1)) {
      val xPartition = partitioner.partition(x, Int.MaxValue)
      val yPartition = partitioner.partition(y, Int.MaxValue)

      // Could have false positives, but it's not likely
      xPartition shouldNot be(yPartition)
    }
  }
}
