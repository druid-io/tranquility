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
import com.twitter.util.Return
import com.twitter.util.Throw
import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
  * Beams accept messages and forward them along.
  */
trait Beam[MessageType]
{
  /**
    * Request propagation of messages. The return value indicates the number of messages known to be sent
    * successfully. Note that for some implementations, it is possible for a message to be sent and for the ack to
    * be lost (e.g. if the send is occurring over a network).
    *
    * If the returned Future resolves to a DefunctBeamException, this means the beam should be discarded
    * (after calling close()).
    *
    * @param messages a batch of messages
    * @return the number of messages propagated
    */
  @deprecated("use sendBatch", "0.7.0")
  final def propagate(messages: Seq[MessageType]): Future[Int] = {
    Future.collectToTry(sendAll(messages)) map { results =>
      results count {
        case Return(result) => result.sent
        case Throw(e) => throw e
      }
    }
  }

  /**
    * Request propagation of messages. The returned bitset contains the indexes of messages known to be sent
    * successfully. Note that for some implementations, it is possible for a message to be sent and for the ack to be
    * lost (e.g. if the send is occurring over a network).
    *
    * If the returned Future resolves to a DefunctBeamException, this means the beam should be discarded
    * (after calling close()).
    *
    * @param messages a batch of messages
    * @return a bitset containing indexes of messages that were sent successfully
    */
  @deprecated("use sendAll", "0.8.0")
  final def sendBatch(messages: Seq[MessageType]): Future[BitSet] = {
    Future.collectToTry(sendAll(messages)) map { results =>
      val merged = mutable.BitSet()
      for ((tryResult, idx) <- results.zipWithIndex) {
        tryResult match {
          case Return(result) =>
            if (result.sent) {
              merged += idx
            }

          case Throw(e) =>
            throw e
        }
      }
      merged.toImmutable
    }
  }

  /**
    * Request propagation of messages. The returned futures contains the results of sending these messages, in a Seq
    * in the same order as the original messages. Note that for some implementations, it is possible for a message to
    * be sent and for the ack to be lost (e.g. if the send is occurring over a network).
    *
    * If any of the the returned Futures resolves to a DefunctBeamException, this means the beam should be discarded
    * (after calling close()).
    *
    * @param messages a batch of messages
    * @return futures containing send result of each message
    */
  def sendAll(messages: Seq[MessageType]): Seq[Future[SendResult]]

  /**
    * Signal that no more messages will be sent. Many implementations need this to do proper cleanup. This operation
    * may happen asynchronously.
    *
    * @return future that resolves when closing is complete
    */
  def close(): Future[Unit]
}

sealed abstract class SendResult extends Equals
{
  /**
    * True if the message was confirmed sent, false if we believe the message to be dropped.
    */
  def sent: Boolean

  override def hashCode(): Int = if (sent) 1 else 0

  override def canEqual(other: Any): Boolean = other.isInstanceOf[SendResult]

  override def equals(other: Any): Boolean = other match {
    case that: SendResult => sent == that.sent
    case _ => false
  }
}

object SendResult
{
  val Sent: SendResult = new SendResult {
    override def sent: Boolean = true
  }

  val Dropped: SendResult = new SendResult {
    override def sent: Boolean = false
  }
}

class DefunctBeamException(s: String, t: Throwable) extends Exception(s, t)
{
  def this(s: String) = this(s, null)
}
