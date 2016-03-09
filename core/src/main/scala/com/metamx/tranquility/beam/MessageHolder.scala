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

package com.metamx.tranquility.beam

import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.typeclass.ObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import org.joda.time.DateTime
import scala.collection.mutable

class MessageHolder[A](
  val message: A,
  timestamper: Timestamper[A],
  partitioner: Partitioner[A]
) extends Equals
{
  lazy val timestamp: DateTime = timestamper.timestamp(message)

  // numPartitions -> cached partition
  private val cachedPartitions = mutable.HashMap[Int, Int]()

  def partition(numPartitions: Int): Int = {
    cachedPartitions.synchronized {
      cachedPartitions.getOrElseUpdate(numPartitions, partitioner.partition(message, numPartitions))
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[MessageHolder[_]]

  override def equals(other: Any): Boolean = other match {
    case that: MessageHolder[_] => message == that.message
    case _ => false
  }

  override def hashCode(): Int = {
    message.hashCode()
  }

  override def toString = s"MessageHolder($message)"
}

object MessageHolder
{
  implicit val Timestamper: Timestamper[MessageHolder[_]] = {
    new Timestamper[MessageHolder[_]]
    {
      override def timestamp(a: MessageHolder[_]): DateTime = a.timestamp
    }
  }

  val Partitioner: Partitioner[MessageHolder[_]] = {
    new Partitioner[MessageHolder[_]]
    {
      override def partition(a: MessageHolder[_], numPartitions: Int): Int = a.partition(numPartitions)
    }
  }

  def wrapObjectWriter[A](underlying: ObjectWriter[A]): ObjectWriter[MessageHolder[A]] = {
    new ObjectWriter[MessageHolder[A]]
    {
      override def asBytes(obj: MessageHolder[A]): Array[Byte] = {
        underlying.asBytes(obj.message)
      }

      override def batchAsBytes(objects: TraversableOnce[MessageHolder[A]]): Array[Byte] = {
        underlying.batchAsBytes(objects.map(_.message))
      }

      override def contentType: String = {
        underlying.contentType
      }
    }
  }

  def apply[A](message: A, timestamper: Timestamper[A], partitioner: Partitioner[A]) = {
    new MessageHolder(message, timestamper, partitioner)
  }
}
