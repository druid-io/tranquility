package com.metamx.tranquility.test

import com.fasterxml.jackson.core.JsonGenerator
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.tranquility.beam.{BeamPacketizer, BeamPacketizerListener, MemoryBeam}
import com.metamx.tranquility.typeclass.JsonWriter
import java.util.concurrent.atomic.AtomicLong
import org.scalatest.FunSuite

class BeamPacketizerTest extends FunSuite with Logging
{
  def newPacketizer(queueSize: Int): (AtomicLong, AtomicLong, BeamPacketizer[String, Dict]) = {
    val beam = new MemoryBeam[Dict](
      "foo",
      new JsonWriter[Dict]
      {
        override protected def viaJsonGenerator(a: Dict, jg: JsonGenerator): Unit = {
          Jackson.generate(a, jg)
        }
      }
    )

    val acked = new AtomicLong()
    val failed = new AtomicLong()
    val listener = new BeamPacketizerListener[String] {
      override def ack(a: String): Unit = {
        acked.incrementAndGet()
      }

      override def fail(a: String): Unit = {
        failed.incrementAndGet()
      }
    }

    (acked, failed, new BeamPacketizer[String, Dict](beam, s => Dict("bar" -> s), listener, queueSize))
  }

  test("Simple") {
    for (queueSize <- Seq(1, 2, 100)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(100)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      packetizer.flush()
      assert(acked.get() === 2, "acked (queueSize = %d)" format queueSize)
      assert(failed.get() === 0, "failed (queueSize = %d)" format queueSize)
      assert(
        MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
        "output (queueSize = %d)" format queueSize
      )
      packetizer.stop()
    }
  }
}
