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

import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.MemoryBeam
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers

class SimpleTranquilizerAdapterTest extends FunSuite with ShouldMatchers
{
  test("Simple") {
    val sender = TranquilizerTest.newTranquilizer(TranquilizerTest.newImmediateMemoryBeam(), 100, 1, 0)._1.simple()

    try {
      MemoryBeam.clear()
      Seq("hey", "what") foreach sender.send
      sender.flush()
      assert(MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")))
    }
    finally {
      sender.stop()
    }
  }

  test("Drops, without drop reporting") {
    val sender = TranquilizerTest.newTranquilizer(TranquilizerTest.newImmediateMemoryBeam(), 100, 1, 0)._1.simple()

    try {
      MemoryBeam.clear()
      Seq("hey", "__drop__") foreach sender.send
      sender.flush()
      assert(MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey")))
    }
    finally {
      sender.stop()
    }
  }

  test("Drops, with drop reporting") {
    val sender = TranquilizerTest.newTranquilizer(TranquilizerTest.newImmediateMemoryBeam(), 100, 1, 0)._1.simple(true)

    try {
      a[MessageDroppedException] should be thrownBy {
        MemoryBeam.clear()
        Seq("hey", "__drop__") foreach sender.send
        sender.flush()
      }
      assert(MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey")))
    }
    finally {
      sender.stop()
    }
  }

  test("Exceptions") {
    val sender = TranquilizerTest.newTranquilizer(TranquilizerTest.newImmediateMemoryBeam(), 100, 1, 0)._1.simple()

    try {
      an[IllegalStateException] should be thrownBy {
        MemoryBeam.clear()
        Seq("hey", "__fail__") foreach sender.send
        sender.flush()
      }
    }
    finally {
      sender.stop()
    }
  }
}
