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

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import io.druid.data.input.MapBasedInputRow
import io.druid.data.input.impl.TimestampSpec
import io.druid.query.aggregation.LongSumAggregatorFactory
import javax.ws.rs.core.MediaType
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.collection.JavaConverters._

class InputRowObjectWriterTest extends FunSuite with ShouldMatchers
{
  val mapBasedRow = new MapBasedInputRow(
    new DateTime("2000"),
    Seq("foo", "baz").asJava,
    Map[String, AnyRef](
      "foo" -> "x",
      "bar" -> Int.box(2),
      "baz" -> "what",
      "hey" -> "there"
    ).asJava
  )

  val objectWriter = new InputRowObjectWriter(
    new TimestampSpec("ts", "millis", null),
    Vector(new LongSumAggregatorFactory("barr", "bar")),
    Nil,
    Jackson.newObjectMapper(),
    MediaType.APPLICATION_JSON
  )

  test("asBytes") {
    Jackson.parse[Dict](objectWriter.asBytes(mapBasedRow)) should be(
      Dict(
        "ts" -> new DateTime("2000").getMillis,
        "bar" -> 2,
        "foo" -> "x",
        "baz" -> "what"
      )
    )
  }

  test("batchAsBytes") {
    Jackson.parse[Seq[Dict]](objectWriter.batchAsBytes(Seq(mapBasedRow))) should be(
      Seq(
        Dict(
          "ts" -> new DateTime("2000").getMillis,
          "bar" -> 2,
          "foo" -> "x",
          "baz" -> "what"
        )
      )
    )
  }

  test("contentType") {
    objectWriter.contentType should be("application/json")
  }
}
