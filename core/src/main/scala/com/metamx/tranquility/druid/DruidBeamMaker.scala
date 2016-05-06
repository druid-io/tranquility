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
package com.metamx.tranquility.druid

import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.Granularity
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.BeamMaker
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.typeclass.ObjectWriter
import com.twitter.util.Await
import com.twitter.util.Future
import io.druid.data.input.impl.TimestampSpec
import java.{util => ju}
import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.chrono.ISOChronology
import org.scala_tools.time.Implicits._
import scala.util.Random

class DruidBeamMaker[A](
  config: DruidBeamConfig,
  location: DruidLocation,
  beamTuning: ClusteredBeamTuning,
  druidTuningMap: Dict,
  rollup: DruidRollup,
  timestampSpec: TimestampSpec,
  taskLocator: TaskLocator,
  indexService: IndexService,
  emitter: ServiceEmitter,
  objectWriter: ObjectWriter[A],
  druidObjectMapper: ObjectMapper
) extends BeamMaker[A, DruidBeam[A]] with Logging
{
  private[tranquility] def taskBytes(
    interval: Interval,
    availabilityGroup: String,
    firehoseId: String,
    partition: Int,
    replicant: Int
  ): Array[Byte] =
  {
    val dataSource = location.dataSource
    val suffix = if (config.randomizeTaskId) {
      // Randomize suffix to allow creation of multiple tasks with the same parameters (useful for testing)
      val rand = Random.nextInt()
      val suffix0 = (0 until 8).map(i => (rand >> (i * 4)) & 0x0F).map(n => ('a' + n).toChar).mkString
      "_%s" format suffix0
    } else {
      ""
    }
    val taskId = "index_realtime_%s_%s_%s_%s%s" format(dataSource, interval.start, partition, replicant, suffix)
    val shutoffTime = interval.end + beamTuning.windowPeriod + config.firehoseGracePeriod
    val queryGranularityMap = druidObjectMapper.convertValue(
      rollup.indexGranularity,
      classOf[ju.Map[String, AnyRef]]
    )
    val dataSchemaMap = Map(
      "dataSource" -> dataSource,
      "parser" -> Map(
        "type" -> "map",
        "parseSpec" -> Map(
          "format" -> "json",
          "timestampSpec" -> timestampSpec,
          "dimensionsSpec" -> rollup.dimensions.specMap
        )
      ),
      "metricsSpec" -> Jackson.parse[Seq[Dict]](druidObjectMapper.writeValueAsBytes(rollup.aggregators.toArray)),
      "granularitySpec" -> Map(
        "type" -> "uniform",
        "segmentGranularity" -> beamTuning.segmentGranularity,
        "queryGranularity" -> queryGranularityMap
      )
    )
    val ioConfigMap = Map(
      "type" -> "realtime",
      "plumber" -> null,
      "firehose" -> Map(
        "type" -> "clipped",
        "interval" -> interval,
        "delegate" -> Map(
          "type" -> "timed",
          "shutoffTime" -> shutoffTime,
          "delegate" -> Map(
            "type" -> "receiver",
            "serviceName" -> location.environment.firehoseServicePattern.format(firehoseId),
            "bufferSize" -> config.firehoseBufferSize
          )
        )
      )
    )
    val druidTuningMapWithOverrides = druidTuningMap ++ Map(
      "windowPeriod" -> beamTuning.windowPeriod.toString(),
      "shardSpec" -> Map(
        "type" -> "linear",
        "partitionNum" -> partition
      ),
      "rejectionPolicy" -> (if (beamTuning.maxSegmentsPerBeam > 1) {
        // Experimental setting, can cause tasks to cover many hours. We still want handoff to occur mid-task,
        // so we need a non-noop rejection policy. Druid won't tell us when it rejects events due to its
        // rejection policy, so this breaks the contract of Beam.propagate telling the user when events are and
        // are not dropped. This is bad, so, only use this rejection policy when we absolutely need to.
        Map("type" -> "serverTime")
      } else {
        Map("type" -> "none")
      })
    )
    // Warn if anything from the tuningMap is getting overridden.
    for ((k, v) <- druidTuningMap) {
      if (druidTuningMapWithOverrides(k) != v) {
        log.warn(s"DruidTuning key[$k] for task[$taskId] overridden from[$v] to[${druidTuningMapWithOverrides(k)}].")
      }
    }
    val taskMap = Map(
      "type" -> "index_realtime",
      "id" -> taskId,
      "resource" -> Map(
        "availabilityGroup" -> availabilityGroup,
        "requiredCapacity" -> 1
      ),
      "spec" -> Map(
        "dataSchema" -> dataSchemaMap,
        "ioConfig" -> ioConfigMap,
        "tuningConfig" -> druidTuningMapWithOverrides
      )
    )
    druidObjectMapper.writeValueAsBytes(normalizeJava(taskMap))
  }

  override def newBeam(interval: Interval, partition: Int) = {
    require(
      beamTuning.segmentGranularity.widen(interval) == interval,
      "Interval does not match segmentGranularity[%s]: %s" format(beamTuning.segmentGranularity, interval)
    )
    val baseFirehoseId = DruidBeamMaker.generateBaseFirehoseId(
      location.dataSource,
      beamTuning.segmentGranularity,
      interval.start,
      partition
    )
    val availabilityGroup = DruidBeamMaker.generateAvailabilityGroup(location.dataSource, interval.start, partition)
    val futureTasks = for (replicant <- 0 until beamTuning.replicants) yield {
      val firehoseId = "%s-%04d" format(baseFirehoseId, replicant)
      indexService.submit(taskBytes(interval, availabilityGroup, firehoseId, partition, replicant)) map {
        taskId =>
          TaskPointer(taskId, firehoseId)
      }
    }
    val tasks = Await.result(Future.collect(futureTasks))
    new DruidBeam(
      interval,
      partition,
      tasks,
      location,
      config,
      taskLocator,
      indexService,
      emitter,
      objectWriter
    )
  }

  override def toDict(beam: DruidBeam[A]) = {
    // At some point we started allowing beams to cover more than one segment.
    // We'll attempt to be backwards compatible when possible.
    val canBeBackwardsCompatible = beamTuning.segmentBucket(beam.interval.start) == beam.interval
    Dict(
      "interval" -> beam.interval.toString(),
      "partition" -> beam.partition,
      "tasks" -> (beam.tasks map {
        task =>
          Dict("id" -> task.id, "firehoseId" -> task.serviceKey)
      })
    ) ++ (if (canBeBackwardsCompatible) Dict("timestamp" -> beam.interval.start.toString()) else Map.empty)
  }

  override def fromDict(d: Dict) = {
    val interval = if (d contains "interval") {
      new Interval(d("interval"), ISOChronology.getInstanceUTC)
    } else {
      // Backwards compatibility (see toDict).
      beamTuning.segmentBucket(new DateTime(d("timestamp"), ISOChronology.getInstanceUTC))
    }
    require(
      beamTuning.segmentGranularity.widen(interval) == interval,
      "Interval does not match segmentGranularity[%s]: %s" format(beamTuning.segmentGranularity, interval)
    )
    val partition = int(d("partition"))
    val tasks = if (d contains "tasks") {
      list(d("tasks")).map(dict(_)).map(d => TaskPointer(str(d("id")), str(d("firehoseId"))))
    } else {
      Seq(TaskPointer(str(d("taskId")), str(d("firehoseId"))))
    }
    new DruidBeam(
      interval,
      partition,
      tasks,
      location,
      config,
      taskLocator,
      indexService,
      emitter,
      objectWriter
    )
  }
}

object DruidBeamMaker
{
  def generateAvailabilityGroup(dataSource: String, ts: DateTime, partition: Int): String = {
    "%s-%s-%04d".format(dataSource, ts.withChronology(ISOChronology.getInstanceUTC), partition)
  }

  def generateBaseFirehoseId(
    dataSource: String,
    segmentGranularity: Granularity,
    ts: DateTime,
    partition: Int
  ): String =
  {
    // Not only is this a nasty hack, it also only works if the RT task hands things off in a timely manner. We'd rather
    // use UUIDs, but this creates a ton of clutter in service discovery.

    val tsUtc = new DateTime(ts.millis, ISOChronology.getInstanceUTC)

    val cycleBucket = segmentGranularity match {
      case Granularity.MINUTE => tsUtc.hourOfDay().get % 3 * 60 + tsUtc.minuteOfHour().get // 180 buckets
      case Granularity.FIVE_MINUTE => tsUtc.hourOfDay().get % 3 * 60 + tsUtc.minuteOfHour().get // 36 buckets
      case Granularity.TEN_MINUTE => tsUtc.hourOfDay().get % 3 * 60 + tsUtc.minuteOfHour().get // 18 buckets
      case Granularity.FIFTEEN_MINUTE => tsUtc.hourOfDay().get % 3 * 60 + tsUtc.minuteOfHour().get // 12 buckets
      case Granularity.HOUR => tsUtc.hourOfDay().get
      case Granularity.SIX_HOUR => tsUtc.hourOfDay().get
      case Granularity.DAY => tsUtc.dayOfMonth().get
      case Granularity.WEEK => tsUtc.weekOfWeekyear().get
      case Granularity.MONTH => tsUtc.monthOfYear().get
      case Granularity.YEAR => tsUtc.yearOfCentury().get
      case x => throw new IllegalArgumentException("No gross firehose id hack for granularity[%s]" format x)
    }

    "%s-%03d-%04d".format(dataSource, cycleBucket, partition)
  }
}
