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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.metamx.common.Granularity
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.TestingTimekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam._
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import java.util.UUID
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.collection.immutable.BitSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClusteredBeamTest extends FunSuite with CuratorRequiringSuite with BeforeAndAfter with Matchers
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

  val _beams    = new ArrayBuffer[TestingBeam]()
  val _buffers  = mutable.HashMap[String, EventBuffer]()
  val _lock     = new AnyRef
  val localZone = new DateTime().getZone

  def buffers = _lock.synchronized {
    _buffers.values.map(x => (x.timestamp.withZone(localZone), x.partition, x.open, x.buffer.toSeq)).toSet
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
    override def sendAll(events: Seq[SimpleEvent]): Seq[Future[SendResult]] = {
      beam.sendAll(events)
    }

    def blockagate(events: Seq[SimpleEvent]): BitSet = {
      Await.result(beam.sendBatch(events))
    }

    override def close() = {
      beam.close()
    }
  }

  class TestingBeam(val timestamp: DateTime, val partition: Int, val uuid: String = UUID.randomUUID().toString)
    extends Beam[SimpleEvent]
  {
    _lock.synchronized {
      _beams += this
    }

    final def sendAll(_events: Seq[SimpleEvent]): Seq[Future[SendResult]] = _lock.synchronized {
      val result = if (_events.contains(events("defunct"))) {
        Future.exception(new DefunctBeamException("Defunct"))
      } else {
        val buffer = _buffers.getOrElseUpdate(uuid, new EventBuffer(timestamp, partition))
        buffer.open = true
        buffer.buffer ++= _events
        Future(SendResult.Sent)
      }
      val promises = _events.map(_ => Promise[SendResult]())
      promises foreach (_.become(result))
      promises
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
      val timestamp = new DateTime(d("timestamp"))
      val partition = int(d("partition"))
      val uuid = str(d("uuid"))
      new TestingBeam(timestamp, partition, uuid)
    }
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

  val defaultTuning = ClusteredBeamTuning(
    segmentGranularity = Granularity.HOUR,
    warmingPeriod = 0.minutes,
    windowPeriod = 10.minutes,
    partitions = 2,
    replicants = 1,
    minSegmentsPerBeam = 1,
    maxSegmentsPerBeam = 1
  )

  before {
    _lock.synchronized {
      _beams.clear()
      _buffers.clear()
    }
  }

  test("Expiration") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, defaultTuning)

        // First set of events
        beams.timekeeper.now = start
        beams.blockagate(Seq("a", "b") map events) should be(Set(1))
        beams.blockagate(Seq("c", "d") map events) should be(Set(0, 1))
        beams.blockagate(Seq("e", "f", "g") map events) should be(Set(0, 1))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d") map events),
            (new DateTime("2012-01-01T01Z"), 1, true, Seq("e", "f") map events)
          )
        )

        // Move forward in time, send the same events
        beams.timekeeper.now = start + 2.hours
        beams.blockagate(Seq("a", "b") map events) should be(Set())
        beams.blockagate(Seq("c", "d") map events) should be(Set())
        beams.blockagate(Seq("e", "f", "g") map events) should be(Set(2))
        beams.blockagate(Seq("h") map events) should be(Set(0))
        assert(
          buffers === Set(
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

  test("Multi") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, defaultTuning)
        val beamsB = newBeams(curator, defaultTuning)

        beamsA.timekeeper.now = start
        beamsB.timekeeper.now = start

        beamsA.blockagate(Seq("b") map events) should be(Set(0))
        beamsB.blockagate(Seq("b") map events) should be(Set(0))
        beamsB.blockagate(Seq("d") map events) should be(Set(0))
        beamsA.blockagate(Seq("c") map events) should be(Set(0))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("d", "c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
    }
  }

  test("Failover") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, defaultTuning)
        val beamsB = newBeams(curator, defaultTuning)
        val beamsC = newBeams(curator, defaultTuning)

        beamsA.timekeeper.now = start
        beamsB.timekeeper.now = start
        beamsC.timekeeper.now = start

        beamsA.blockagate(Seq("c") map events) should be(Set(0))
        Await.result(beamsA.close())

        beamsB.blockagate(Seq("d") map events) should be(Set(0))
        beamsC.blockagate(Seq("e") map events) should be(Set(0))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "e") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
    }
  }

  test("ScaleUp") {
    withLocalCurator {
      curator =>
        val oldTuning = defaultTuning
        val newTuning = oldTuning.copy(partitions = oldTuning.partitions + 1)

        val beamsA = newBeams(curator, oldTuning)
        beamsA.timekeeper.now = start
        beamsA.blockagate(Seq("c") map events) should be(Set(0))
        Await.result(beamsA.close())

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, false, Seq("c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )

        val beamsB = newBeams(curator, newTuning)
        beamsB.timekeeper.now = start
        beamsB.blockagate(Seq("d", "b") map events) should be(Set(0, 1))
        beamsB.blockagate(Seq("d", "c", "b") map events) should be(Set(0, 1, 2))
        beamsB.blockagate(Seq("c", "b") map events) should be(Set(0, 1))
        beamsB.blockagate(Seq("c", "b") map events) should be(Set(0, 1))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
            (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 2, true, Seq("b") map events),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
            (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c") map events),
            (new DateTime("2012-01-01T01Z"), 2, true, Seq("c") map events)
          )
        )
    }
  }

  test("ScaleDown") {
    withLocalCurator {
      curator =>
        val oldTuning = defaultTuning
        val newTuning = oldTuning.copy(partitions = oldTuning.partitions - 1)

        val beamsA = newBeams(curator, oldTuning)
        beamsA.timekeeper.now = start
        beamsA.blockagate(Seq("c") map events) should be(Set(0))
        Await.result(beamsA.close())

        val beamsB = newBeams(curator, newTuning)
        beamsB.timekeeper.now = start
        beamsB.blockagate(Seq("d", "b") map events) should be(Set(0, 1))
        beamsB.blockagate(Seq("d", "c", "b") map events) should be(Set(0, 1, 2))
        beamsB.blockagate(Seq("c", "b") map events) should be(Set(0, 1))
        beamsB.blockagate(Seq("c", "b") map events) should be(Set(0, 1))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b", "b", "b") map events),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "d", "c") map events),
            (new DateTime("2012-01-01T01Z"), 1, true, Seq("d", "c", "c") map events)
          )
        )
    }
  }

  test("DefunctBeam") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, defaultTuning)
        beams.timekeeper.now = start
        beams.blockagate(Seq("b", "c") map events) should be(Set(0, 1))
        beams.blockagate(Seq("defunct") map events) should be(Set())
        beams.blockagate(Seq("b", "c") map events) should be(Set(0))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, true, Seq("b") map events),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
    }
  }

  test("Warming") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, defaultTuning.copy(warmingPeriod = 10.minutes, windowPeriod = 6.minutes))
        beams.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        beams.blockagate(Seq("b") map events) should be(Set(0))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, false, Nil),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
        val desired = List("2012-01-01T00Z", "2012-01-01T00Z", "2012-01-01T01Z", "2012-01-01T01Z").map(new DateTime(_))
        val startTime = System.currentTimeMillis()
        while (System.currentTimeMillis() < startTime + 2000 &&
          beamsList.map(_.timestamp).sortBy(_.getMillis) != desired) {
          Thread.sleep(100)
        }
        assert(beamsList.map(_.timestamp).sortBy(_.getMillis) === desired)
    }
  }

  test("WarmingConcurrency") {
    withLocalCurator {
      curator =>
        val beamsA = newBeams(curator, defaultTuning.copy(warmingPeriod = 10.minutes, windowPeriod = 6.minutes))
        val beamsB = newBeams(curator, defaultTuning.copy(windowPeriod = 6.minutes))
        beamsA.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        beamsB.timekeeper.now = new DateTime("2012-01-01T00:55Z")
        beamsA.blockagate(Seq("b") map events) should be(Set(0))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, false, Nil),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
        beamsA.blockagate(Seq("c") map events) should be(Set(0))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
        beamsB.blockagate(Seq("b") map events) should be(Set(0))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
        beamsB.blockagate(Seq("c") map events) should be(Set(0))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil),
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "c") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Nil)
          )
        )
    }
  }

  test("NoBeamNecromancy") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, defaultTuning)

        beams.timekeeper.now = start
        beams.blockagate(Seq("c") map events) should be(Set(0))

        beams.timekeeper.now = start + 1.hour
        beams.blockagate(Seq("d") map events) should be(Set(0))

        beams.timekeeper.now = start
        beams.blockagate(Seq("e") map events) should be(Set(0))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
            (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
          )
        )

        beams.timekeeper.now = start + 2.hours
        beams.blockagate(Seq("g") map events) should be(Set(0))
        beams.blockagate(Seq("g") map events) should be(Set(0))
        beams.blockagate(Seq("b") map events) should be(Set())
        beams.blockagate(Seq("b") map events) should be(Set())
        beams.blockagate(Seq("c") map events) should be(Set())

        beams.timekeeper.now = start
        beams.blockagate(Seq("f") map events) should be(Set())
        beams.blockagate(Seq("f") map events) should be(Set())
        beams.blockagate(Seq("b") map events) should be(Set())
        beams.blockagate(Seq("b") map events) should be(Set())
        beams.blockagate(Seq("c") map events) should be(Set())

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
            (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
            (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
          )
        )
    }
  }

  test("NoBeamNecromancyWhenRestarting") {
    withLocalCurator {
      curator =>
        val beams = newBeams(curator, defaultTuning)

        beams.timekeeper.now = start
        beams.blockagate(Seq("c") map events) should be(Set(0))

        beams.timekeeper.now = start + 1.hour
        beams.blockagate(Seq("d") map events) should be(Set(0))

        beams.timekeeper.now = start
        beams.blockagate(Seq("e") map events) should be(Set(0))

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, true, Seq("c", "e") map events),
            (new DateTime("2012-01-01T01Z"), 1, true, Seq("d") map events)
          )
        )

        Await.result(beams.close())

        val beams2 = newBeams(curator, defaultTuning)
        beams2.timekeeper.now = start + 2.hours
        beams2.blockagate(Seq("g") map events) should be(Set(0))
        beams2.blockagate(Seq("g") map events) should be(Set(0))
        beams2.blockagate(Seq("b") map events) should be(Set())
        beams2.blockagate(Seq("c") map events) should be(Set())

        beams2.timekeeper.now = start
        beams2.blockagate(Seq("f") map events) should be(Set())
        beams2.blockagate(Seq("f") map events) should be(Set())
        beams2.blockagate(Seq("b") map events) should be(Set())
        beams2.blockagate(Seq("c") map events) should be(Set())

        assert(
          buffers === Set(
            (new DateTime("2012-01-01T01Z"), 0, false, Seq("c", "e") map events),
            (new DateTime("2012-01-01T01Z"), 1, false, Seq("d") map events),
            (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
            (new DateTime("2012-01-01T03Z"), 1, true, Seq("g") map events)
          )
        )
    }
  }

  test("SegmentsPerBeam") {
    withLocalCurator {
      curator =>
        val tuning = defaultTuning.copy(minSegmentsPerBeam = 3, maxSegmentsPerBeam = 3, windowPeriod = 5.hours)
        val beams = newBeams(curator, tuning)

        beams.timekeeper.now = start
        beams.blockagate(Seq("b", "c") map events) should be(Set(0, 1))
        beams.blockagate(Seq("d", "g") map events) should be(Set(0, 1))
        beams.blockagate(Seq("d", "h") map events) should be(Set(0, 1))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "c", "d") map events),
            (new DateTime("2012-01-01T00Z"), 1, true, Seq("d") map events),
            (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
            (new DateTime("2012-01-01T03Z"), 1, true, Seq("h") map events)
          )
        )
    }
  }

  test("SegmentsPerBeam, Restoring") {
    withLocalCurator {
      curator =>
        val tuning = defaultTuning.copy(minSegmentsPerBeam = 3, maxSegmentsPerBeam = 3, windowPeriod = 5.hours)
        val beamsA = newBeams(curator, tuning)
        val beamsB = newBeams(curator, tuning)

        beamsA.timekeeper.now = start
        beamsB.timekeeper.now = start

        beamsA.blockagate(Seq("b") map events) should be(Set(0))
        Await.result(beamsA.close())
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, false, Seq("b") map events),
            (new DateTime("2012-01-01T00Z"), 1, false, Nil)
          )
        )

        beamsB.blockagate(Seq("c") map events) should be(Set(0))
        beamsB.blockagate(Seq("d", "g") map events) should be(Set(0, 1))
        beamsB.blockagate(Seq("d", "h") map events) should be(Set(0, 1))
        assert(
          buffers === Set(
            (new DateTime("2012-01-01T00Z"), 0, true, Seq("b", "c", "d") map events),
            (new DateTime("2012-01-01T00Z"), 1, true, Seq("d") map events),
            (new DateTime("2012-01-01T03Z"), 0, true, Seq("g") map events),
            (new DateTime("2012-01-01T03Z"), 1, true, Seq("h") map events)
          )
        )
    }
  }

  test("MetaSerde") {
    val s =
      """{
        |   "beams" : {
        |      "2000-01-01T20:30:00.000+05:30" : [
        |         {
        |            "partition" : 123
        |         }
        |      ]
        |   },
        |   "latestCloseTime" : "2000-01-01T19:30:00.000+05:30"
        |}""".stripMargin
    def checkMeta(meta: ClusteredBeamMeta) {
      assert(meta.latestCloseTime === new DateTime("2000-01-01T14:00:00.000Z").withZone(DateTimeZone.UTC))
      assert(
        meta.beamDictss.keys.toSet === Set(
          new DateTime("2000-01-01T15:00:00.000Z").getMillis
        )
      )
      assert(meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z").getMillis).size === 1)
      assert(meta.beamDictss(new DateTime("2000-01-01T15:00:00.000Z").getMillis)(0)("partition") === 123)
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
