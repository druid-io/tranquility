/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.metamx.tranquility.beam

import com.google.common.hash.Hashing
import com.metamx.common.scala.Logging
import com.twitter.util.Future

/**
 * Partitions events based on their hashCode modulo the number of delegate beams, and propagates the partitioned events
 * via the appropriate beam.
 */
class HashPartitionBeam[A](
  val delegates: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  def propagate(events: Seq[A]) = {
    val futures = events.groupBy(event => Hashing.consistentHash(event.hashCode, delegates.size)) map {
      case (i, group) =>
        delegates(i).propagate(group)
    }
    Future.collect(futures.toList).map(_.sum)
  }

  def close() = {
    Future.collect(delegates map (_.close())) map (_ => ())
  }

  override def toString = "HashPartitionBeam(%s)" format delegates.mkString(", ")
}
