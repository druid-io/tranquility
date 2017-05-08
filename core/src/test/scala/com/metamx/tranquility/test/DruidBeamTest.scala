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

import _root_.io.druid.data.input.impl.TimestampSpec
import _root_.io.druid.granularity.QueryGranularities
import _root_.io.druid.indexing.common.task.RealtimeIndexTask
import _root_.io.druid.indexing.common.task.Task
import _root_.io.druid.query.aggregation.LongSumAggregatorFactory
import _root_.io.druid.segment.realtime.firehose.ChatHandlerProvider
import _root_.io.druid.segment.realtime.firehose.ClippedFirehoseFactory
import _root_.io.druid.segment.realtime.firehose.NoopChatHandlerProvider
import _root_.io.druid.server.metrics.EventReceiverFirehoseRegister
import _root_.io.druid.timeline.partition.LinearShardSpec
import _root_.scala.collection.JavaConverters._
import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.InjectableValues
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectReader
import com.github.nscala_time.time.Imports._
import com.google.inject.Key
import com.metamx.common.Granularity
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.core.NoopEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.druid._
import org.joda.time.chrono.ISOChronology
import org.scalatest.FunSuite
import org.scalatest.Matchers

class DruidBeamTest extends FunSuite with Matchers
{
  private def emptyEmitter(): ServiceEmitter = new ServiceEmitter(
    "service", "host", new NoopEmitter()
  )

  private def defaultObjectReader(): ObjectReader = DruidGuicer.Default.objectMapper.reader(
    new InjectableValues
    {
      override def findInjectableValue(
          valueId: Any,
          ctxt: DeserializationContext,
          forProperty: databind.BeanProperty,
          beanInstance: scala.Any
        ): AnyRef =
      {
        valueId match {
          case k: Key[_] if k.getTypeLiteral.getRawType == classOf[ChatHandlerProvider] => new NoopChatHandlerProvider
          case k: Key[_] if k.getTypeLiteral.getRawType == classOf[ObjectMapper] => DruidGuicer.Default.objectMapper
          case k: Key[_] if k.getTypeLiteral.getRawType == classOf[EventReceiverFirehoseRegister] =>
            new EventReceiverFirehoseRegister
        }
      }
    }
  ).withType(classOf[Task])

  test("GenerateAvailabilityGroup") {
    val dt = new DateTime("2010-02-03T04:34:56.789", DateTimeZone.forID("America/Los_Angeles"))
    assert(DruidBeamMaker.generateAvailabilityGroup("x", dt, 1) === "x-2010-02-03T12:34:56.789Z-0001")
  }

  test("GenerateFirehoseId: H=00") {
    val dt = new DateTime("2010-02-03T00:34:56.789Z")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SECOND, dt, 1) === "x-296-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIVE_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.TEN_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIFTEEN_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) === "x-000-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SIX_HOUR, dt, 1) === "x-000-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) === "x-003-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.WEEK, dt, 1) === "x-005-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MONTH, dt, 1) === "x-002-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.YEAR, dt, 1) === "x-010-0001")
  }

  test("GenerateFirehoseId: H=12") {
    val dt = new DateTime("2010-02-03T12:34:56.789Z")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SECOND, dt, 1) === "x-296-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIVE_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.TEN_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIFTEEN_MINUTE, dt, 1) === "x-034-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) === "x-012-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SIX_HOUR, dt, 1) === "x-012-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) === "x-003-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.WEEK, dt, 1) === "x-005-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MONTH, dt, 1) === "x-002-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.YEAR, dt, 1) === "x-010-0001")
  }

  test("GenerateFirehoseId: H=23") {
    val dt = new DateTime("2010-02-03T23:34:56.789Z")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SECOND, dt, 1) === "x-296-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) === "x-154-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIVE_MINUTE, dt, 1) === "x-154-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.TEN_MINUTE, dt, 1) === "x-154-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIFTEEN_MINUTE, dt, 1) === "x-154-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) === "x-023-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SIX_HOUR, dt, 1) === "x-023-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) === "x-003-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.WEEK, dt, 1) === "x-005-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MONTH, dt, 1) === "x-002-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.YEAR, dt, 1) === "x-010-0001")
  }

  test("Task JSON") {
    val druidBeamMaker = new DruidBeamMaker[Dict](
      DruidBeamConfig(),
      DruidLocation.create("druid/overlord", "mydatasource"),
      ClusteredBeamTuning(
        segmentGranularity = Granularity.HOUR,
        warmingPeriod = 0.minutes,
        windowPeriod = 4.minutes
      ),
      DruidTuning(
        maxRowsInMemory = 100,
        intermediatePersistPeriod = 3.minutes,
        maxPendingPersists = 3
      ).toMap,
      DruidRollup(
        dimensions = SpecificDruidDimensions(Seq("dim1", "dim2"), Seq(DruidSpatialDimension.singleField("spatial1"))),
        aggregators = Seq(new LongSumAggregatorFactory("met1", "met1")),
        indexGranularity = QueryGranularities.MINUTE,
        true
      ),
      new TimestampSpec("ts", "iso", null),
      null,
      null,
      emptyEmitter(),
      null,
      DruidGuicer.Default.objectMapper
    )
    val interval = new Interval("2000/PT1H", ISOChronology.getInstanceUTC)
    val taskBytes = druidBeamMaker.taskBytes(
      interval,
      "mygroup",
      "myfirehose",
      1,
      2
    )
    val task = defaultObjectReader().readValue(taskBytes).asInstanceOf[RealtimeIndexTask]
    task.getId should be("index_realtime_mydatasource_2000-01-01T00:00:00.000Z_1_2")
    task.getDataSource should be("mydatasource")
    task.getTaskResource.getAvailabilityGroup should be("mygroup")

    val tuningConfig = task.getRealtimeIngestionSchema.getTuningConfig
    tuningConfig.getWindowPeriod should be(4.minutes.toPeriod)
    tuningConfig.getMaxRowsInMemory should be(100)
    tuningConfig.getIntermediatePersistPeriod should be(3.minutes.toPeriod)
    tuningConfig.getMaxPendingPersists should be(3)
    tuningConfig.getShardSpec shouldBe a[LinearShardSpec]
    tuningConfig.getShardSpec.getPartitionNum should be(1)

    val ioConfig = task.getRealtimeIngestionSchema.getIOConfig
    ioConfig.getPlumberSchool should be(null)
    ioConfig.getFirehoseFactoryV2 should be(null)
    ioConfig.getFirehoseFactory.asInstanceOf[ClippedFirehoseFactory].getInterval
      .withChronology(ISOChronology.getInstanceUTC) should be(interval)

    val dataSchema = task.getRealtimeIngestionSchema.getDataSchema
    dataSchema.getDataSource should be("mydatasource")
    dataSchema.getAggregators.deep should be(Array(new LongSumAggregatorFactory("met1", "met1")).deep)
    dataSchema.getGranularitySpec.getSegmentGranularity should be(Granularity.HOUR)
    dataSchema.getGranularitySpec.getQueryGranularity should be(QueryGranularities.MINUTE)

    val parseSpec = dataSchema.getParser.getParseSpec
    parseSpec.getTimestampSpec.getTimestampColumn should be("ts")
    parseSpec.getTimestampSpec.getTimestampFormat should be("iso")
    parseSpec.getDimensionsSpec.getDimensions.asScala.map(_.getName) should be(Seq("dim1", "dim2", "spatial1"))
    parseSpec.getDimensionsSpec.getSpatialDimensions.asScala.map(_.getDimName) should be(Seq("spatial1"))
  }

  test("Attribute isRollup should be passed to task") {
    val isRollup = false
    val druidBeamMaker = new DruidBeamMaker[Dict](
      DruidBeamConfig(),
      DruidLocation.create("druid/overlord", "mydatasource"),
      ClusteredBeamTuning(),
      DruidTuning().toMap,
      DruidRollup(
        dimensions = SpecificDruidDimensions(Seq(), Seq()),
        aggregators = Seq(),
        indexGranularity = QueryGranularities.NONE,
        // isRollup is set for test.
        isRollup
      ),
      new TimestampSpec("ts", "iso", null),
      null,
      null,
      emptyEmitter(),
      null,
      DruidGuicer.Default.objectMapper
    )

    val interval = new Interval("2000/PT1H", ISOChronology.getInstanceUTC)
    val taskBytes = druidBeamMaker.taskBytes(
      interval,
      "mygroup",
      "myfirehose",
      1,
      2
    )
    val task = defaultObjectReader().readValue(taskBytes).asInstanceOf[RealtimeIndexTask]

    task.getRealtimeIngestionSchema.getDataSchema.getGranularitySpec.isRollup should be(isRollup)
  }
}
