/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.metamx.tranquility.test

import com.fasterxml.jackson.core.JsonGenerator
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.BeamPacketizer
import com.metamx.tranquility.beam.BeamPacketizerListener
import com.metamx.tranquility.beam.MemoryBeam
import com.metamx.tranquility.typeclass.JsonWriter
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicLong
import org.scalatest.FunSuite

class BeamPacketizerTest extends FunSuite with Logging
{
  def newPacketizer(
    batchSize: Int,
    maxPendingBatches: Int
  ): (AtomicLong, AtomicLong, BeamPacketizer[String]) =
  {
    val memoryBeam = new MemoryBeam[String](
      "foo",
      new JsonWriter[String]
      {
        override protected def viaJsonGenerator(a: String, jg: JsonGenerator): Unit = {
          Jackson.generate(Dict("bar" -> a), jg)
        }
      }
    )
    val beam = new Beam[String] {
      override def propagate(events: Seq[String]) = {
        if (events.contains("__fail__")) {
          Future.exception(new IllegalStateException("fail!"))
        } else {
          memoryBeam.propagate(events)
        }
      }

      override def close() = memoryBeam.close()
    }

    val acked = new AtomicLong()
    val failed = new AtomicLong()
    val listener = new BeamPacketizerListener[String] {
      override def ack(a: String): Unit = {
        acked.incrementAndGet()
      }

      override def fail(e: Throwable, a: String): Unit = {
        failed.incrementAndGet()
      }
    }

    (acked, failed,
      new BeamPacketizer[String](beam, listener, batchSize, maxPendingBatches))
  }

  test("Send by batchSize") {
    for (batchSize <- Seq(1, 2); maxPendingBatches <- Seq(1)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, maxPendingBatches)
      val desc = "(batchSize = %d, maxPendingBatches = %d)" format (batchSize, maxPendingBatches)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      assert(acked.get() === 2, "acked (%s)" format desc)
      assert(failed.get() === 0, "failed (%s)" format desc)
      assert(
        MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
        "output (%s)" format desc
      )
      packetizer.close()
    }
  }

  test("Send by flush()") {
    for (batchSize <- Seq(1, 2, 100); maxPendingBatches <- Seq(200)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, maxPendingBatches)
      val desc = "(batchSize = %d, maxPendingBatches = %d)" format (batchSize, maxPendingBatches)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      assert(acked.get() === 0, "acked (%s)" format desc)
      assert(failed.get() === 0, "failed (%s)" format desc)
      packetizer.flush()
      assert(acked.get() === 2, "acked (%s)" format desc)
      assert(failed.get() === 0, "failed (%s)" format desc)
      assert(
        MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
        "output (%s)" format desc
      )
      packetizer.close()
    }
  }

  test("Send with failures (single event batches)") {
    for (batchSize <- Seq(1); maxPendingBatches <- Seq(1)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, maxPendingBatches)
      val desc = "(batchSize = %d, maxPendingBatches = %d)" format (batchSize, maxPendingBatches)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("__fail__")
      assert(acked.get() === 1, "acked (%s)" format desc)
      assert(failed.get() === 1, "failed (%s)" format desc)
      packetizer.close()
    }
  }

  test("Send with failures (multi event batches)") {
    for (batchSize <- Seq(2); maxPendingBatches <- Seq(1)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, maxPendingBatches)
      val desc = "(batchSize = %d, maxPendingBatches = %d)" format (batchSize, maxPendingBatches)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("__fail__")
      assert(acked.get() === 0, "acked (%s)" format desc)
      assert(failed.get() === 2, "failed (%s)" format desc)
      packetizer.close()
    }
  }
}
