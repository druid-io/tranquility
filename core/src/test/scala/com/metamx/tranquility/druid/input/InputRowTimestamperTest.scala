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

import io.druid.data.input.MapBasedInputRow
import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.collection.JavaConverters._

class InputRowTimestamperTest extends FunSuite with ShouldMatchers
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

  val timestamper = InputRowTimestamper.Instance

  test("timestamp") {
    timestamper.timestamp(row) should be(new DateTime("2000"))
  }
}
