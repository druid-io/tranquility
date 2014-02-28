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
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.{ClusteredBeamTuning, RoundRobinBeam, ClusteredBeamMeta, ClusteredBeam, BeamMaker, DefunctBeamException, Beam}
import com.metamx.tranquility.test.traits.CuratorRequiringSpec
import com.metamx.tranquility.typeclass.Timestamper
import com.simple.simplespec.Spec
import com.twitter.util.{Await, Future}
import java.util.UUID
import org.apache.curator.framework.CuratorFramework
import org.joda.time.{Interval, DateTime}
import org.junit.{Before, Ignore, Test}
import org.scala_tools.time.Implicits._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClusteredBeamUnitSpec extends Spec with CuratorRequiringSpec
{
  @Ignore
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

  val _beams = new ArrayBuffer[TestingBeam]() with mutable.SynchronizedBuffer[TestingBeam]

  val _buffers = ConcurrentMap[String, EventBuffer]()

  def buffers = _buffers.values.map(x => (x.timestamp, x.partition, x.open, x.buffer.toSeq)).toSet

  @Ignore
  class EventBuffer(val timestamp: DateTime, val partition: Int)
  {
    val buffer: mutable.Buffer[SimpleEvent] = mutable.ListBuffer()
    @volatile var open: Boolean = true
  }

  @Ignore
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

  @Ignore
  class TestingBeam(val timestamp: DateTime, val partition: Int, val uuid: String = UUID.randomUUID().toString)
    extends Beam[SimpleEvent]
  {
    _beams += this

    def propagate(_events: Seq[SimpleEvent]) = {
      if (_events.contains(events("defunct"))) {
        Future.exception(new DefunctBeamException("Defunct"))
      } else {
        val buffer = _buffers.getOrElseUpdate(uuid, new EventBuffer(timestamp, partition))
        buffer.open = true
        buffer.buffer ++= _events
        Future.value(_events.size)
      }
    }

    def close() = {
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

  @Ignore
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

  class BeamTests
  {
    val tuning = new ClusteredBeamTuning(Granularity.HOUR, 0.minutes, 10.minutes, 2, 1)

    @Before
    def setUp() {
      _beams.clear()
    }

    @Test
    def testExpiration() {
      withLocalCurator {
        curator =>
          val beams = newBeams(curator, tuning)

          // First set of events
          beams.timekeeper.now = start
          beams.blockagate(Seq("a", "b") map events)
          beams.blockagate(Seq("c", "d") map events)
          beams.blockagate(Seq("e", "f", "g") map events)
          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d") map events),
              (new DateTime("2012-01-01T01Z"), 1, true, Seq("e", "f") map events)
            )
          )

          // Move forward in time, send the same events
          beams.timekeeper.now = start + 2.hours
          beams.blockagate(Seq("a", "b") map events)
          beams.blockagate(Seq("c", "d") map events)
          beams.blockagate(Seq("e", "f", "g") map events)
          beams.blockagate(Seq("h") map events)
          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, false, Seq("b") map events),
              (new DateTime("2012-01-01T00Z"), 1, false, Nil),
              (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "d") map events),
              (new DateTime("2012-01-01T01Z"), 1, false, Seq("e", "f") map events),
              (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
              (new DateTime("2012-01-01T03Z"), 1, true, Seq("h") map events)
            )
          )
      }
    }

    @Test
    def testMulti() {
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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("d", "c") map events)
            )
          )
      }
    }

    @Test
    def testFailover() {
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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "e") map events),
              (new DateTime("2012-01-01T01Z"), 1, false, Nil)
            )
          )
      }
    }

    @Test
    def testScaleUp() {
      withLocalCurator {
        curator =>
          val oldTuning = tuning
          val newTuning = oldTuning.copy(partitions = oldTuning.partitions + 1)

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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
              (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
              (new DateTime("2012-01-01T00Z"), 2, true, Seq("b") map events),
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
              (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c", "c") map events)
            )
          )
      }
    }

    @Test
    def testScaleDown() {
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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b", "b", "b") map events),
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
              (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c", "c") map events)
            )
          )
      }
    }

    @Test
    def testDefunctBeam() {
      withLocalCurator {
        curator =>
          val beams = newBeams(curator, tuning)
          beams.timekeeper.now = start
          beams.blockagate(Seq("b", "c") map events) must be(2)
          beams.blockagate(Seq("defunct") map events) must be(0)
          beams.blockagate(Seq("b", "c") map events) must be(1)

          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
              (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events)
            )
          )
      }
    }

    @Test
    def testWarming() {
      withLocalCurator {
        curator =>
          val beams = newBeams(curator, tuning.copy(warmingPeriod = 10.minutes, windowPeriod = 6.minutes))
          beams.timekeeper.now = start
          beams.blockagate(Seq("b") map events) must be(1)
          buffers must be(
            Set(
              (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events)
            )
          )
          val desired = List("2012-01-01T00Z", "2012-01-01T00Z", "2012-01-01T01Z", "2012-01-01T01Z").map(new DateTime(_))
          val startTime = System.currentTimeMillis()
          while (System.currentTimeMillis() < startTime + 2000 && _beams.toList.map(_.timestamp).sortBy(_.millis) != desired) {
            Thread.sleep(100)
          }
          _beams.toList.map(_.timestamp).sortBy(_.millis) must be(desired)
      }
    }

    @Test
    def testNoBeamNecromancy() {
      withLocalCurator {
        curator =>
          val beams = newBeams(curator, tuning)

          beams.timekeeper.now = start
          beams.blockagate(Seq("c") map events)

          beams.timekeeper.now = start + 1.hour
          beams.blockagate(Seq("d") map events)

          beams.timekeeper.now = start
          beams.blockagate(Seq("e") map events)

          buffers must be(
            Set(
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
              (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
            )
          )

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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
              (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
              (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
              (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
            )
          )
      }
    }

    @Test
    def testNoBeamNecromancyWhenRestarting() {
      withLocalCurator {
        curator =>
          val beams = newBeams(curator, tuning)

          beams.timekeeper.now = start
          beams.blockagate(Seq("c") map events)

          beams.timekeeper.now = start + 1.hour
          beams.blockagate(Seq("d") map events)

          beams.timekeeper.now = start
          beams.blockagate(Seq("e") map events)

          buffers must be(
            Set(
              (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
              (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
            )
          )

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

          buffers must be(
            Set(
              (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
              (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
              (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
              (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
            )
          )
      }
    }
  }

  class MetaSerdeTests
  {
    @Test
    def testSimple() {
      val s = """{
                |   "beams" : {
                |      "2000-01-01T15:00:00.000Z" : [
                |         {
                |            "partition" : 123
                |         }
                |      ]
                |   },
                |   "latestTime" : "2000-01-01T15:00:00.000Z"
                |}""".stripMargin
      def checkMeta(meta: ClusteredBeamMeta) {
        meta.latestTime must be(new DateTime("2000-01-01T15:00:00.000Z"))
        meta.beamDictss.keys.toSet must be(
          Set(
            new DateTime("2000-01-01T15:00:00.000Z")
          )
        )
        meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z")).size must be(1)
        meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z"))(0)("partition") must be(123)
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

}
