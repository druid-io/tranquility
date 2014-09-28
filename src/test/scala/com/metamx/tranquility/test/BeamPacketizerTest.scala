package com.metamx.tranquility.test

import com.fasterxml.jackson.core.JsonGenerator
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.{Jackson, Logging}
import com.metamx.tranquility.beam.{BeamPacketizer, BeamPacketizerListener, MemoryBeam}
import com.metamx.tranquility.typeclass.JsonWriter
import java.util.concurrent.atomic.AtomicLong
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

class BeamPacketizerTest extends FunSuite with Logging
{
  def newPacketizer(
    batchSize: Int,
    queueSize: Int,
    emitMillis: Long
  ): (AtomicLong, AtomicLong, BeamPacketizer[String, Dict]) =
  {
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

    (acked, failed,
      new BeamPacketizer[String, Dict](beam, s => Dict("bar" -> s), listener, batchSize, queueSize, emitMillis))
  }

  test("Send by flush()") {
    for (batchSize <- Seq(1, 2, 100); queueSize <- Seq(1000); emitMillis <- Seq(0L, 1L, 300000000L)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, queueSize, emitMillis)
      val desc = "(batchSize = %d, queueSize = %d, emitMillis = %d)" format (batchSize, queueSize, emitMillis)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      packetizer.flush()
      assert(acked.get() === 2, "acked (%s)" format desc)
      assert(failed.get() === 0, "failed (%s)" format desc)
      assert(
        MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
        "output (%s)" format desc
      )
      packetizer.stop()
    }
  }

  test("Send by batchSize") {
    for (batchSize <- Seq(1, 2); queueSize <- Seq(2, 1000); emitMillis <- Seq(0L, 1L, 300000000L)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, queueSize, emitMillis)
      val desc = "(batchSize = %d, queueSize = %d, emitMillis = %d)" format (batchSize, queueSize, emitMillis)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      within(10.seconds) {
        assert(acked.get() === 2, "acked (%s)" format desc)
        assert(failed.get() === 0, "failed (%s)" format desc)
        assert(
          MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
          "output (%s)" format desc
        )
      }
      packetizer.stop()
    }
  }

  test("Send by emitMillis") {
    for (batchSize <- Seq(1000); queueSize <- Seq(1000); emitMillis <- Seq(1L, 500L)) {
      MemoryBeam.clear()
      val (acked, failed, packetizer) = newPacketizer(batchSize, queueSize, emitMillis)
      val desc = "(batchSize = %d, queueSize = %d, emitMillis = %d)" format (batchSize, queueSize, emitMillis)
      packetizer.start()
      packetizer.send("hey")
      packetizer.send("what")
      within(10.seconds) {
        assert(acked.get() === 2, "acked (%s)" format desc)
        assert(failed.get() === 0, "failed (%s)" format desc)
        assert(
          MemoryBeam.get("foo") === Seq(Dict("bar" -> "hey"), Dict("bar" -> "what")),
          "output (%s)" format desc
        )
      }
      packetizer.stop()
    }
  }

  def within[A](timeout: Period)(f: => A): A = {
    val end = DateTime.now + timeout
    var result: Option[A] = None
    while (result.isEmpty) {
      try {
        result = Some(f)
      } catch {
        case e: TestFailedException =>
          if (DateTime.now <= end) {
            // Suppress, try again soon.
            Thread.sleep(100)
          } else {
            throw e
          }
      }
    }
    result.get
  }
}
