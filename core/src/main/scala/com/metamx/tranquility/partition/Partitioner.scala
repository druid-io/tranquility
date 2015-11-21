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

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet

/**
  * Class that knows how to partition objects of a certain type into an arbitrary number of buckets. This is
  * generally used along with a MergingPartitioningBeam to create a beamMergeFn.
  */
trait Partitioner[-A] extends Serializable
{
  def partition(a: A, numPartitions: Int): Int
}

object Partitioner
{
  /**
    * Helper function for use by Partitioner implementations. Computes a hash code derived from a message's
    * truncated timestamp and its dimensions.
    *
    * @param truncatedTimestamp timestamp, truncated to indexGranularity
    * @param dimensions iterable of (dim name, dim value) tuples. The dim value can be a String,
    * @return
    */
  def timeAndDimsHashCode(truncatedTimestamp: Long, dimensions: Iterable[(String, Any)]): Int = {
    val hasher = Hashing.murmur3_32().newHasher()
    hasher.putLong(truncatedTimestamp)

    for ((dimName, dimValue) <- SortedMap(dimensions.toSeq: _*)) {
      val dimValueAsStrings: Set[String] = dimValue match {
        case x: String if x.nonEmpty =>
          Set(x)

        case x: Number =>
          Set(String.valueOf(x))

        case xs: ju.List[_] =>
          SortedSet(xs.asScala.filterNot(s => s == null).map(String.valueOf): _*).filterNot(_.isEmpty)

        case xs: Seq[_] =>
          SortedSet(xs.filterNot(s => s == null).map(String.valueOf): _*).filterNot(_.isEmpty)

        case _ =>
          Set.empty
      }

      // This is not a one-to-one mapping, but that's okay, since we're only using it for hashing and
      // not for an equality check. Some moderate collisions are fine.
      if (dimValueAsStrings.nonEmpty) {
        hasher.putBytes(dimName.getBytes(Charsets.UTF_8))
        for (s <- dimValueAsStrings) {
          hasher.putBytes(s.getBytes(Charsets.UTF_8))
        }
      }
    }

    hasher.hash().asInt()
  }
}
