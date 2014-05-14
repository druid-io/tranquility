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
package com.metamx.tranquility.druid

import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.{ClusteredBeamTuning, BeamMaker}
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.typeclass.{JsonWriter, Timestamper}
import com.twitter.util.{Future, Await}
import io.druid.data.input.impl.{TimestampSpec, MapInputRowParser, SpatialDimensionSchema}
import io.druid.indexing.common.index.EventReceiverFirehoseFactory
import io.druid.indexing.common.task.{Task, TaskResource, RealtimeIndexTask}
import io.druid.segment.realtime.firehose.{TimedShutoffFirehoseFactory, ClippedFirehoseFactory}
import io.druid.segment.realtime.plumber.NoopRejectionPolicyFactory
import io.druid.segment.realtime.{FireDepartmentConfig, Schema}
import io.druid.timeline.partition.LinearShardSpec
import org.joda.time.{Interval, DateTime}
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._
import scala.util.Random

class DruidBeamMaker[A : Timestamper](
  config: DruidBeamConfig,
  location: DruidLocation,
  tuning: ClusteredBeamTuning,
  rollup: DruidRollup,
  timestampSpec: TimestampSpec,
  finagleRegistry: FinagleRegistry,
  indexService: IndexService,
  emitter: ServiceEmitter,
  timekeeper: Timekeeper,
  jsonWriter: JsonWriter[A]
) extends BeamMaker[A, DruidBeam[A]] with Logging
{
  private def taskObject(
    ts: DateTime,
    availabilityGroup: String,
    firehoseId: String,
    partition: Int,
    replicant: Int
  ): Task =
  {
    // Randomize suffix to allow creation of multiple tasks with the same parameters (useful for testing)
    val rand = Random.nextInt()
    val suffix = (0 until 8).map(i => (rand >> (i*4)) & 0x0F).map(n => ('a' + n).toChar).mkString
    val taskId = "index_realtime_%s_%s_%s_%s_%s" format (location.dataSource, ts, partition, replicant, suffix)
    val interval = tuning.segmentBucket(ts)
    val shutoffTime = interval.end + tuning.windowPeriod + config.firehoseGracePeriod
    val shardSpec = new LinearShardSpec(partition)
    val parser = {
      val dimensions = rollup.dimensions match {
        case SpecificDruidDimensions(xs) =>
          // Sort dimension names to trick IndexMerger into working correctly (it assumes row comparator based on
          // lexicographically ordered dimension names, which will only be the case if we force it here)
          Some(xs.sorted)
        case SchemalessDruidDimensions(xs) =>
          // null dimensions causes the Druid parser to go schemaless
          None
      }
      val dimensionExclusions = rollup.dimensions match {
        case SpecificDruidDimensions(xs) => None
        case SchemalessDruidDimensions(xs) => Some(xs)
      }
      new MapInputRowParser(
        timestampSpec,
        dimensions.map(_.asJava).orNull,
        dimensionExclusions.map(_.asJava).orNull
      )
    }
    new RealtimeIndexTask(
      taskId,
      new TaskResource(availabilityGroup, 1),
      new Schema(
        location.dataSource,
        List.empty[SpatialDimensionSchema].asJava,
        rollup.aggregators.toArray,
        rollup.indexGranularity,
        shardSpec
      ),
      new ClippedFirehoseFactory(
        new TimedShutoffFirehoseFactory(
          new EventReceiverFirehoseFactory(
            location.environment.firehoseServicePattern format firehoseId,
            null,
            parser,
            null
          ), shutoffTime
        ), interval
      ),
      new FireDepartmentConfig(75000, 10.minutes),
      tuning.windowPeriod,
      1, // TODO: put into config
      tuning.segmentGranularityAsIndexGranularity,
      new NoopRejectionPolicyFactory
    )
  }

  override def newBeam(interval: Interval, partition: Int) = {
    require(
      tuning.segmentBucket(interval.start) == interval,
      "Interval does not match segmentGranularity[%s]: %s" format(tuning.segmentGranularity, interval)
    )
    val availabilityGroup = DruidBeamMaker.generateBaseFirehoseId(
      location.dataSource,
      tuning.segmentGranularity,
      interval.start,
      partition
    )
    val futureTasks = for (replicant <- 0 until tuning.replicants) yield {
      val firehoseId = "%s-%04d" format(availabilityGroup, replicant)
      indexService.submit(taskObject(interval.start, availabilityGroup, firehoseId, partition, replicant)) map {
        taskId =>
          DruidTaskPointer(taskId, firehoseId)
      }
    }
    val tasks = Await.result(Future.collect(futureTasks))
    new DruidBeam(
      interval.start,
      partition,
      tasks,
      location,
      config,
      finagleRegistry,
      indexService,
      emitter,
      timekeeper,
      jsonWriter
    )
  }

  override def toDict(beam: DruidBeam[A]) = Dict(
    "timestamp" -> beam.timestamp.toString(),
    "partition" -> beam.partition,
    "tasks" -> (beam.tasks map {
      task =>
        Dict("id" -> task.id, "firehoseId" -> task.firehoseId)
    })
  )

  override def fromDict(d: Dict) = {
    val ts = new DateTime(d("timestamp"))
    require(
      tuning.segmentGranularity.truncate(ts) == ts,
      "Timestamp does not match segmentGranularity[%s]: %s" format(tuning.segmentGranularity, ts)
    )
    val partition = partitionFromDict(d)
    val tasks = if (d contains "tasks") {
      list(d("tasks")).map(dict(_)).map(d => DruidTaskPointer(str(d("id")), str(d("firehoseId"))))
    } else {
      Seq(DruidTaskPointer(str(d("taskId")), str(d("firehoseId"))))
    }
    new DruidBeam(
      ts,
      partition,
      tasks,
      location,
      config,
      finagleRegistry,
      indexService,
      emitter,
      timekeeper,
      jsonWriter
    )
  }

  override def intervalFromDict(d: Dict) = tuning.segmentBucket(new DateTime(d("timestamp")))

  override def partitionFromDict(d: Dict) = int(d("partition"))
}

object DruidBeamMaker
{
  def generateBaseFirehoseId(dataSource: String, segmentGranularity: Granularity, ts: DateTime, partition: Int) = {
    // Not only is this a nasty hack, it also only works if the RT task hands things off in a timely manner. We'd rather
    // use UUIDs, but this creates a ton of clutter in service discovery.

    val cycleBucket = segmentGranularity match {
      case Granularity.MINUTE => ts.minuteOfHour.get
      case Granularity.HOUR => ts.hourOfDay.get
      case Granularity.DAY => ts.dayOfMonth.get
      case x => throw new IllegalArgumentException("No gross firehose id hack for granularity[%s]" format x)
    }

    "%s-%02d-%04d".format(dataSource, cycleBucket, partition)
  }
}
