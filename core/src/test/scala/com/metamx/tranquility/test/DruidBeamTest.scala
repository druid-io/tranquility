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

import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.InjectableValues
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Key
import com.metamx.common.Granularity
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.core.Emitter
import com.metamx.emitter.core.Event
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.druid.DruidBeamConfig
import com.metamx.tranquility.druid.DruidBeamMaker
import com.metamx.tranquility.druid.DruidGuicer
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.DruidSpatialDimension
import com.metamx.tranquility.druid.DruidTuning
import com.metamx.tranquility.druid.SpecificDruidDimensions
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularity
import io.druid.indexing.common.task.RealtimeIndexTask
import io.druid.indexing.common.task.Task
import io.druid.query.aggregation.LongSumAggregatorFactory
import io.druid.segment.realtime.firehose.ChatHandlerProvider
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider
import io.druid.timeline.partition.LinearShardSpec
import org.joda.time.chrono.ISOChronology
import org.scala_tools.time.Imports._
import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.collection.JavaConverters._

class DruidBeamTest extends FunSuite with Matchers
{
  test("GenerateFirehoseId") {
    val dt = new DateTime("2010-02-03T12:34:56.789Z")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) === "x-34-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIVE_MINUTE, dt, 1) === "x-34-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.TEN_MINUTE, dt, 1) === "x-34-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.FIFTEEN_MINUTE, dt, 1) === "x-34-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) === "x-12-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.SIX_HOUR, dt, 1) === "x-12-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) === "x-03-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.WEEK, dt, 1) === "x-05-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MONTH, dt, 1) === "x-02-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.YEAR, dt, 1) === "x-10-0001")
  }

  test("Task JSON") {
    val timestamper = null
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
      ),
      DruidRollup(
        dimensions = SpecificDruidDimensions(Seq("dim1", "dim2"), Seq(DruidSpatialDimension.singleField("spatial1"))),
        aggregators = Seq(new LongSumAggregatorFactory("met1", "met1")),
        indexGranularity = QueryGranularity.MINUTE
      ),
      new TimestampSpec("ts", "iso", null),
      null,
      null,
      new ServiceEmitter(
        "service", "host", new Emitter
        {
          override def flush(): Unit = ???

          override def emit(event: Event): Unit = ???

          override def close(): Unit = ???

          override def start(): Unit = ???
        }
      ),
      null,
      DruidGuicer.Default.objectMapper
    )(timestamper)
    val interval = new Interval("2000/PT1H", ISOChronology.getInstanceUTC)
    val taskBytes = druidBeamMaker.taskBytes(
      interval,
      "mygroup",
      "myfirehose",
      1,
      2
    )
    val objectReader = DruidGuicer.Default.objectMapper.reader(
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
          }
        }
      }
    ).withType(classOf[Task])

    val task = objectReader.readValue(taskBytes).asInstanceOf[RealtimeIndexTask]
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
    dataSchema.getGranularitySpec.getQueryGranularity should be(QueryGranularity.MINUTE)

    val parseSpec = dataSchema.getParser.getParseSpec
    parseSpec.getTimestampSpec.getTimestampColumn should be("ts")
    parseSpec.getTimestampSpec.getTimestampFormat should be("iso")
    parseSpec.getDimensionsSpec.getDimensions.asScala should be(Seq("dim1", "dim2"))
    parseSpec.getDimensionsSpec.getSpatialDimensions.asScala.map(_.getDimName) should be(Seq("spatial1"))
  }
}
