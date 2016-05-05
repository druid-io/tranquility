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

import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.beam.MemoryBeam
import com.metamx.tranquility.beam.SendResult
import com.metamx.tranquility.test.common.FailableBeam
import com.metamx.tranquility.typeclass.DefaultJsonWriter
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.collection.immutable.BitSet

class BeamTest extends FunSuite with BeforeAndAfter with ShouldMatchers with Logging
{
  val Key  = "beep"
  val beam = FailableBeam.forDicts(new MemoryBeam[Dict](Key, new DefaultJsonWriter(Jackson.newObjectMapper())))

  before {
    MemoryBeam.clear()
  }

  test("propagate") {
    val future = beam.propagate(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "qux", FailableBeam.Drop),
        Dict("foo" -> "hey")
      )
    )

    Await.result(future) should be(3)
    MemoryBeam.get(Key).sortBy(d => String.valueOf(d("foo"))) should be(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "hey")
      )
    )
  }

  test("propagate, with failures") {
    val future = beam.propagate(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "qux", FailableBeam.Drop),
        Dict("foo" -> "buh", FailableBeam.Fail),
        Dict("foo" -> "hey")
      )
    )

    an[IllegalStateException] should be thrownBy {
      Await.result(future)
    }
  }

  test("sendBatch") {
    val future = beam.sendBatch(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "qux", FailableBeam.Drop),
        Dict("foo" -> "hey")
      )
    )

    Await.result(future) should be(BitSet(0, 1, 3))
    MemoryBeam.get(Key).sortBy(d => String.valueOf(d("foo"))) should be(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "hey")
      )
    )
  }

  test("sendBatch, with failures") {
    val future = beam.sendBatch(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "qux", FailableBeam.Drop),
        Dict("foo" -> "buh", FailableBeam.Fail),
        Dict("foo" -> "hey")
      )
    )

    an[IllegalStateException] should be thrownBy {
      Await.result(future)
    }

    MemoryBeam.get(Key).sortBy(d => String.valueOf(d("foo"))) should be(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "hey")
      )
    )
  }

  test("sendAll") {
    val futures = beam.sendAll(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "qux", FailableBeam.Drop),
        Dict("foo" -> "buh", FailableBeam.Fail),
        Dict("foo" -> "hey")
      )
    )

    val results = Await.result(Future.collectToTry(futures))
    results.size should be(5)
    results(0) should be(Return(SendResult.Sent))
    results(1) should be(Return(SendResult.Sent))
    results(2) should be(Return(SendResult.Dropped))
    results(3) shouldBe a[Throw[_]]
    results(3).asInstanceOf[Throw[_]].e shouldBe an[IllegalStateException]
    results(4) should be(Return(SendResult.Sent))
    MemoryBeam.get(Key).sortBy(d => String.valueOf(d("foo"))) should be(
      Seq(
        Dict("foo" -> "bar"),
        Dict("foo" -> "baz"),
        Dict("foo" -> "hey")
      )
    )
  }
}
