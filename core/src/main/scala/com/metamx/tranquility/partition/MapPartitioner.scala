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

package com.metamx.tranquility.partition

import com.google.common.hash.Hashing
import com.metamx.common.scala.Logging
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}
import scala.collection.JavaConverters._

object MapPartitioner
{
  /**
    * Create a Partitioner that can partition Scala and Java Maps according to their Druid grouping key (truncated
    * timestamp and dimensions). For other types, you should implement your own Partitioner that accesses your
    * object's fields directly.
    */
  def create[A](
    timestamper: Timestamper[A],
    timestampSpec: TimestampSpec,
    rollup: DruidRollup
  ): Partitioner[A] =
  {
    new MapPartitioner[A](
      timestamper,
      timestampSpec,
      rollup
    )
  }
}

class MapPartitioner[A](
  timestamper: Timestamper[A],
  timestampSpec: TimestampSpec,
  rollup: DruidRollup
) extends Partitioner[A] with Logging
{
  @transient private val didWarn = new AtomicBoolean

  override def partition(thing: A, numPartitions: Int): Int = {
    val dimensions = thing match {
      case scalaMap: collection.Map[_, _] =>
        for ((k: String, v) <- scalaMap if rollup.isStringDimension(timestampSpec, k)) yield {
          (k, v)
        }

      case javaMap: ju.Map[_, _] =>
        for ((k: String, v) <- javaMap.asScala if rollup.isStringDimension(timestampSpec, k)) yield {
          (k, v)
        }

      case obj =>
        // Oops, we don't really know how to do things other than Maps...
        if (didWarn.compareAndSet(false, true)) {
          log.warn(
            "Cannot partition object of class[%s] by time and dimensions. Consider implementing a Partitioner.",
            obj.getClass
          )
        }
        Nil
    }

    val partitionHashCode = if (dimensions.nonEmpty) {
      val truncatedTimestamp = rollup.indexGranularity.truncate(timestamper.timestamp(thing).getMillis)
      Partitioner.timeAndDimsHashCode(truncatedTimestamp, dimensions)
    } else {
      thing.hashCode()
    }

    Hashing.consistentHash(partitionHashCode, numPartitions)
  }
}
