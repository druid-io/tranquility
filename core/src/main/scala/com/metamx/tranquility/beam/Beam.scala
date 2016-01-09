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

import com.twitter.util.Future
import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
  * Beams can accept messages and forward them along. The propagate method may throw a DefunctBeamException, which
  * means the beam should be discarded (after calling close()).
  */
trait Beam[MessageType]
{
  /**
    * Request propagation of messages. The return value indicates the number of messages known to be sent
    * successfully. Note that for some implementations, it is possible for a message to be sent and for the ack to
    * be lost (e.g. if the send is occurring over a network).
    * @param messages a batch of messages
    * @return the number of messages propagated
    */
  @deprecated("use sendBatch", "0.7.0")
  final def propagate(messages: Seq[MessageType]): Future[Int] = {
    sendBatch(messages).map(_.size)
  }

  /**
    * Request propagation of messages. The returned bitset contains the indexes of messages known to be sent
    * successfully. Note that for some implementations, it is possible for a message to be sent and for the ack to be
    * lost (e.g. if the send is occurring over a network).
    * @param messages a batch of messages
    * @return a bitset containing indexes of messages that were sent successfully
    */
  def sendBatch(messages: Seq[MessageType]): Future[BitSet]

  /**
    * Signal that no more messages will be sent. Many implementations need this to do proper cleanup. This operation
    * may happen asynchronously.
    * @return future that resolves when closing is complete
    */
  def close(): Future[Unit]
}

class DefunctBeamException(s: String, t: Throwable) extends Exception(s, t)
{
  def this(s: String) = this(s, null)
}

object Beam
{
  /**
    * Beam.index(xs) is like xs.zipWithIndex, but always returns an IndexedSeq so lookups by index will be efficient.
    * @param xs elements
    * @return indexed seq equivalent to xs.zipWithIndex
    */
  def index[A](xs: Seq[A]): IndexedSeq[(A, Int)] = {
    val indexed = Vector.newBuilder[(A, Int)]
    var i = 0
    for (message <- xs) {
      indexed += ((message, i))
      i += 1
    }
    indexed.result()
  }

  def mergeBitsets[A](bitsets: Iterable[BitSet]): BitSet = {
    val merged = mutable.BitSet()
    bitsets.foreach(merged ++= _)
    merged.toImmutable
  }
}
