/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.tranquility.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.metamx.common.Granularity
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.{Beam, BeamMaker, ClusteredBeam, ClusteredBeamMeta, ClusteredBeamTuning, DefunctBeamException, RoundRobinBeam}
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.{Await, Future}
import org.apache.curator.framework.CuratorFramework
import org.joda.time.{DateTime, Interval}
import org.scala_tools.time.Implicits._
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClusteredBeamTest extends FunSuite with CuratorRequiringSuite with BeforeAndAfter
{
  case class SimpleEvent(ts: DateTime, fields: Map[String, String])

  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }

  val start  = new DateTime("2012-01-01T01:05Z")
  val events = (Seq(
    SimpleEvent(new DateTime("2012-12-31T23:05Z"), Map("foo" -> "a")),
    SimpleEvent(new DateTime("2012-01-01T00:50Z"), Map("foo" -> "b")),
    SimpleEvent(new DateTime("2012-01-01T01:00Z"), Map("foo" -> "c")),
    SimpleEvent(new DateTime("2012-01-01T01:05Z"), Map("foo" -> "d")),
    SimpleEvent(new DateTime("2012-01-01T01:05Z"), Map("foo" -> "defunct")),
    SimpleEvent(new DateTime("2012-01-01T01:10Z"), Map("foo" -> "e")),
    SimpleEvent(new DateTime("2012-01-01T01:20Z"), Map("foo" -> "f")),
    SimpleEvent(new DateTime("2012-01-01T03:05Z"), Map("foo" -> "g")),
    SimpleEvent(new DateTime("2012-01-01T03:20Z"), Map("foo" -> "h"))
  ) map {
    x => x.fields("foo") -> x
  }).toMap

  val _beams = new ArrayBuffer[TestingBeam]()
  val _buffers = mutable.HashMap[String, EventBuffer]()
  val _lock = new AnyRef

  def buffers = _lock.synchronized {
    _buffers.values.map(x => (x.timestamp, x.partition, x.open, x.buffer.toSeq)).toSet
  }

  def beamsList = _lock.synchronized {
    _beams.toList
  }

  class EventBuffer(val timestamp: DateTime, val partition: Int)
  {
    val buffer: mutable.Buffer[SimpleEvent] = mutable.ListBuffer()
    @volatile var open: Boolean = true
  }

  case class TestingBeamsHolder(
    beam: Beam[SimpleEvent],
    beamMaker: TestingBeamMaker,
    timekeeper: TestingTimekeeper
  ) extends Beam[SimpleEvent]
  {
    def propagate(events: Seq[SimpleEvent]) = {
      beam.propagate(events)
    }

    def blockagate(events: Seq[SimpleEvent]): Int = {
      Await.result(beam.propagate(events))
    }

    def close() = {
      beam.close()
    }
  }

  class TestingBeam(val timestamp: DateTime, val partition: Int, val uuid: String = UUID.randomUUID().toString)
    extends Beam[SimpleEvent]
  {
    _lock.synchronized {
      _beams += this
    }

    def propagate(_events: Seq[SimpleEvent]) = _lock.synchronized {
      if (_events.contains(events("defunct"))) {
        Future.exception(new DefunctBeamException("Defunct"))
      } else {
        val buffer = _buffers.getOrElseUpdate(uuid, new EventBuffer(timestamp, partition))
        buffer.open = true
        buffer.buffer ++= _events
        Future.value(_events.size)
      }
    }

    def close() = _lock.synchronized {
      _beams -= this
      val buffer = _buffers.getOrElseUpdate(uuid, new EventBuffer(timestamp, partition))
      buffer.open = false
      Future.Done
    }

    def toDict = Dict(
      "timestamp" -> timestamp.toString(),
      "partition" -> partition,
      "uuid" -> uuid
    )
  }

  class TestingBeamMaker extends BeamMaker[SimpleEvent, TestingBeam]
  {
    def newBeam(interval: Interval, partition: Int) = new TestingBeam(interval.start, partition)

    def toDict(beam: TestingBeam) = {
      Dict(
        "timestamp" -> beam.timestamp.toString(),
        "partition" -> beam.partition,
        "uuid" -> beam.uuid
      )
    }

    def fromDict(d: Dict) = {
      val timestamp = intervalFromDict(d).start
      val partition = partitionFromDict(d)
      val uuid = str(d("uuid"))
      new TestingBeam(timestamp, partition, uuid)
    }

    def intervalFromDict(d: Dict) = {
      val ts = new DateTime(d("timestamp"))
      ts to ts
    }

    def partitionFromDict(d: Dict) = int(d("partition"))
  }

  def newBeams(curator: CuratorFramework, tuning: ClusteredBeamTuning): TestingBeamsHolder = {
    val beamMaker = new TestingBeamMaker
    val timekeeper = new TestingTimekeeper
    val objectMapper = new ObjectMapper withEffect {
      jm =>
        jm.registerModule(DefaultScalaModule)
    }
    val emitter = new ServiceEmitter(
      "service", "host",
      new LoggingEmitter(new Logger(getClass), LoggingEmitter.Level.INFO, Jackson.newObjectMapper()) withEffect {
        _.start()
      }
    )
    TestingBeamsHolder(
      new ClusteredBeam(
        "/beams",
        "ident",
        tuning,
        curator,
        emitter,
        timekeeper,
        objectMapper,
        beamMaker,
        (interval: Interval, partition: Int) => (beam: Beam[SimpleEvent]) => beam,
        (xs: Seq[Beam[SimpleEvent]]) => new RoundRobinBeam(xs.toIndexedSeq),
        Map.empty
      ),
      beamMaker,
      timekeeper
    )
  }

  val tuning = new ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 2, 1)

  before {
    _lock.synchronized {
      _beams.clear()
      _buffers.clear()
    }
  }

  test("Expiration") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, tuning)

        // First set of events
        beams.timekeeper.now = start
        beams.blockagate(Seq("a", "b") map events)
        beams.blockagate(Seq("c", "d") map events)
        beams.blockagate(Seq("e", "f", "g") map events)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d") map events),
          (new DateTime("2012-01-01T01Z"), 1, true, Seq("e", "f") map events)
        ))

        // Move forward in time, send the same events
        beams.timekeeper.now = start + 2.hours
        beams.blockagate(Seq("a", "b") map events)
        beams.blockagate(Seq("c", "d") map events)
        beams.blockagate(Seq("e", "f", "g") map events)
        beams.blockagate(Seq("h") map events)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, false, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "d") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Seq("e", "f") map events),
          (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
          (new DateTime("2012-01-01T03Z"), 1, true, Seq("h") map events)
        ))
    }
  }

  test("Multi") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, tuning)
        val beamsB = newBeams(curator, tuning)

        beamsA.timekeeper.now = start
        beamsB.timekeeper.now = start

        beamsA.blockagate(Seq("b") map events)
        beamsB.blockagate(Seq("b") map events)
        beamsB.blockagate(Seq("d") map events)
        beamsA.blockagate(Seq("c") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("d", "c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
    }
  }

  test("Failover") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, tuning)
        val beamsB = newBeams(curator, tuning)
        val beamsC = newBeams(curator, tuning)

        beamsA.timekeeper.now = start
        beamsB.timekeeper.now = start
        beamsC.timekeeper.now = start

        beamsA.blockagate(Seq("c") map events)
        Await.result(beamsA.close())

        beamsB.blockagate(Seq("d") map events)
        beamsC.blockagate(Seq("e") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "e") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
    }
  }

  test("ScaleUp") {
    withLocalCurator {
      curator =>
        val oldTuning = tuning
        val newTuning = oldTuning.copy(partitions = oldTuning.partitions + 1)

        val beamsA = newBeams(curator, oldTuning)
        beamsA.timekeeper.now = start
        beamsA.blockagate(Seq("c") map events)
        Await.result(beamsA.close())

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, false, Seq("c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))

        val beamsB = newBeams(curator, newTuning)
        beamsB.timekeeper.now = start
        beamsB.blockagate(Seq("d", "b") map events)
        beamsB.blockagate(Seq("d", "c", "b") map events)
        beamsB.blockagate(Seq("c", "b") map events)
        beamsB.blockagate(Seq("c", "b") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
          (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 2, true, Seq("b") map events),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
          (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c") map events),
          (new DateTime("2012-01-01T01Z"), 2, true, Seq("c") map events)
        ))
    }
  }

  test("ScaleDown") {
    withLocalCurator {
      curator =>
        val oldTuning = tuning
        val newTuning = oldTuning.copy(partitions = oldTuning.partitions - 1)

        val beamsA = newBeams(curator, oldTuning)
        beamsA.timekeeper.now = start
        beamsA.blockagate(Seq("c") map events)
        Await.result(beamsA.close())

        val beamsB = newBeams(curator, newTuning)
        beamsB.timekeeper.now = start
        beamsB.blockagate(Seq("d", "b") map events)
        beamsB.blockagate(Seq("d", "c", "b") map events)
        beamsB.blockagate(Seq("c", "b") map events)
        beamsB.blockagate(Seq("c", "b") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b", "b", "b") map events),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
          (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c", "c") map events)
        ))
    }
  }

  test("DefunctBeam") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, tuning)
        beams.timekeeper.now = start
        assert(beams.blockagate(Seq("b", "c") map events) === 2)
        assert(beams.blockagate(Seq("defunct") map events) === 0)
        assert(beams.blockagate(Seq("b", "c") map events) === 1)

        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
    }
  }

  test("Warming") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, tuning.copy(warmingPeriod = 10.minutes, windowPeriod = 6.minutes))
        beams.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        assert(beams.blockagate(Seq("b") map events) === 1)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, false, Nil),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
        val desired = List("2012-01-01T00Z", "2012-01-01T00Z", "2012-01-01T01Z", "2012-01-01T01Z").map(new DateTime(_))
        val startTime = System.currentTimeMillis()
        while (System.currentTimeMillis() < startTime + 2000 && beamsList.map(_.timestamp).sortBy(_.millis) != desired) {
          Thread.sleep(100)
        }
        assert(beamsList.map(_.timestamp).sortBy(_.millis) === desired)
    }
  }

  test("WarmingConcurrency") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, tuning.copy(warmingPeriod = 10.minutes, windowPeriod = 6.minutes))
        val beamsB = newBeams(curator, tuning.copy(windowPeriod = 6.minutes))
        beamsA.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        beamsB.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        assert(beamsA.blockagate(Seq("b") map events) === 1)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, false, Nil),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
        assert(beamsA.blockagate(Seq("c") map events) === 1)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
        assert(beamsB.blockagate(Seq("b") map events) === 1)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
        assert(beamsB.blockagate(Seq("c") map events) === 1)
        assert(buffers === Set(
          (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
          (new DateTime("2012-01-01T00Z"), 1, false, Nil),
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "c") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Nil)
        ))
    }
  }

  test("NoBeamNecromancy") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, tuning)

        beams.timekeeper.now = start
        beams.blockagate(Seq("c") map events)

        beams.timekeeper.now = start + 1.hour
        beams.blockagate(Seq("d") map events)

        beams.timekeeper.now = start
        beams.blockagate(Seq("e") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
          (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
        ))

        beams.timekeeper.now = start + 2.hours
        beams.blockagate(Seq("g") map events)
        beams.blockagate(Seq("g") map events)
        beams.blockagate(Seq("b") map events)
        beams.blockagate(Seq("b") map events)
        beams.blockagate(Seq("c") map events)

        beams.timekeeper.now = start
        beams.blockagate(Seq("f") map events)
        beams.blockagate(Seq("f") map events)
        beams.blockagate(Seq("b") map events)
        beams.blockagate(Seq("b") map events)
        beams.blockagate(Seq("c") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
          (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
          (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
        ))
    }
  }

  test("NoBeamNecromancyWhenRestarting") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, tuning)

        beams.timekeeper.now = start
        beams.blockagate(Seq("c") map events)

        beams.timekeeper.now = start + 1.hour
        beams.blockagate(Seq("d") map events)

        beams.timekeeper.now = start
        beams.blockagate(Seq("e") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
          (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
        ))

        Await.result(beams.close())

        val beams2 = newBeams(curator, tuning)
        beams2.timekeeper.now = start + 2.hours
        beams2.blockagate(Seq("g") map events)
        beams2.blockagate(Seq("g") map events)
        beams2.blockagate(Seq("b") map events)
        beams2.blockagate(Seq("c") map events)

        beams2.timekeeper.now = start
        beams2.blockagate(Seq("f") map events)
        beams2.blockagate(Seq("f") map events)
        beams2.blockagate(Seq("b") map events)
        beams2.blockagate(Seq("c") map events)

        assert(buffers === Set(
          (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
          (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
          (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
          (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
        ))
    }
  }

  test("MetaSerde") {
    val s = """{
              |   "beams" : {
              |      "2000-01-01T15:00:00.000Z" : [
              |         {
              |            "partition" : 123
              |         }
              |      ]
              |   },
              |   "latestCloseTime" : "2000-01-01T14:00:00.000Z"
              |}""".stripMargin
    def checkMeta(meta: ClusteredBeamMeta) {
      assert(meta.latestCloseTime === new DateTime("2000-01-01T14:00:00.000Z"))
      assert(meta.beamDictss.keys.toSet === Set(
          new DateTime("2000-01-01T15:00:00.000Z")
      ))
      assert(meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z")).size === 1)
      assert(meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z"))(0)("partition") === 123)
    }
    val objectMapper = new ObjectMapper withEffect {
      jm =>
        jm.registerModule(DefaultScalaModule)
    }
    val meta1 = ClusteredBeamMeta.fromBytes(objectMapper, s.getBytes(Charsets.UTF_8)).right.get
    checkMeta(meta1)
    checkMeta(ClusteredBeamMeta.fromBytes(objectMapper, meta1.toBytes(objectMapper)).right.get) // Round trip
  }

}
