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

package com.metamx.tranquility.druid.input

import com.google.common.hash.Hashing
import com.metamx.tranquility.partition.Partitioner
import io.druid.data.input.InputRow
import io.druid.granularity.QueryGranularity
import scala.collection.JavaConverters._

/**
  * Partitioner that partitions Druid InputRows by their truncated timestamp and dimensions.
  */
class InputRowPartitioner(queryGranularity: QueryGranularity) extends Partitioner[InputRow]
{
  override def partition(row: InputRow, numPartitions: Int): Int = {
    val partitionHashCode = Partitioner.timeAndDimsHashCode(
      queryGranularity.truncate(row.getTimestampFromEpoch),
      row.getDimensions.asScala.view map { dim =>
        dim -> row.getRaw(dim)
      }
    )

    Hashing.consistentHash(partitionHashCode, numPartitions)
  }
}
