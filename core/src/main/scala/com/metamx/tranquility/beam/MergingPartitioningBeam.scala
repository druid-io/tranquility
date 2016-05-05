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

import com.metamx.common.scala.Logging
import com.metamx.tranquility.partition.Partitioner
import com.twitter.util.Future
import com.twitter.util.Promise

/**
  * Partitions messages based on the output of a Partitioner, and propagates the partitioned messages via the
  * appropriate underlying beams.
  */
class MergingPartitioningBeam[A](
  val partitioner: Partitioner[A],
  val beams: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  override def sendAll(messages: Seq[A]): Seq[Future[SendResult]] = {
    val messagesWithPromises = Vector() ++ messages.map(message => (message, Promise[SendResult]()))
    val grouped: Map[Int, IndexedSeq[(A, Promise[SendResult])]] = messagesWithPromises groupBy { tuple =>
      partitioner.partition(tuple._1, beams.size)
    }
    for {
      (i, group) <- grouped
      ((message, promise), future) <- group zip beams(i).sendAll(group.map(_._1))
    } {
      promise.become(future)
    }
    messagesWithPromises.map(_._2)
  }

  override def close() = {
    Future.collect(beams map (_.close())) map (_ => ())
  }

  override def toString = s"MergingPartitioningBeam(${beams.mkString(", ")})"
}
