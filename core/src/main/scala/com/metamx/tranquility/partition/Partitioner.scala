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

import com.google.common.hash.Hasher
import com.google.common.hash.Hashing
import java.{lang => jl}
import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

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
    * @param dimensions         iterable of (dim name, dim value) tuples. The dim value can be a String,
    *                           Number, or Iterable of anything.
    * @return
    */
  def timeAndDimsHashCode(truncatedTimestamp: Long, dimensions: Iterable[(String, Any)]): Int = {
    val hasher = Hashing.murmur3_32().newHasher()
    hasher.putLong(truncatedTimestamp)

    val b = SortedMap.newBuilder[String, Any] ++= dimensions

    for ((dimName, dimValue) <- b.result()) {
      dimValue match {
        case x: String =>
          // Treat empty strings the same as nulls (ignore them).
          if (x.nonEmpty) {
            addString(hasher, dimName)
            addString(hasher, x)
          }

        case x: Number =>
          addString(hasher, dimName)
          addString(hasher, String.valueOf(x))

        case xs: jl.Iterable[_] =>
          val strings = iterableToSortedStringSet(xs.asScala)
          if (strings.nonEmpty) {
            addString(hasher, dimName)
            for (s <- strings if s != null && !s.isEmpty) {
              addString(hasher, s)
            }
          }

        case xs: Iterable[_] =>
          val strings = iterableToSortedStringSet(xs)
          if (strings.nonEmpty) {
            addString(hasher, dimName)
            for (s <- strings if s != null && !s.isEmpty) {
              addString(hasher, s)
            }
          }

        case _ =>
        // Unknown type, don't include it in the hashCode.
        // This may hurt dispersion but won't hurt correctness.
      }
    }

    hasher.hash().asInt()
  }

  private def iterableToSortedStringSet(xs: Iterable[_]): collection.Set[String] = {
    val iter = xs.iterator
    if (iter.hasNext) {
      val retVal = new ju.TreeSet[String]().asScala
      for (x <- xs) {
        if (x != null) {
          val s = String.valueOf(x)
          if (s.nonEmpty) {
            retVal.add(s)
          }
        }
      }
      retVal
    } else {
      Set.empty
    }
  }

  private def addString(hasher: Hasher, s: String): Unit = {
    hasher.putInt(s.hashCode)
  }
}
