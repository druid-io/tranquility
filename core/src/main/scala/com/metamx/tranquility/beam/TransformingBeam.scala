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
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import scala.collection.mutable.ArrayBuffer

class TransformingBeam[A, B](underlying: Beam[B], f: A => B) extends Beam[A]
{
  override def sendAll(messages: Seq[A]): Seq[Future[SendResult]] = {
    val messagesWithPromises = Vector() ++ messages.map(message => (message, Promise[SendResult]()))
    val sendable = ArrayBuffer[(B, Promise[SendResult])]()

    for ((message, promise) <- messagesWithPromises) {
      Try(f(message)) match {
        case Return(transformedMessage) =>
          sendable += ((transformedMessage, promise))
        case Throw(e) =>
          promise.setException(e)
      }
    }

    // Send sendable messages, and assign their promises.
    for (((message, promise), future) <- sendable zip underlying.sendAll(sendable.map(_._1))) {
      promise.become(future)
    }

    messagesWithPromises.map(_._2)
  }

  override def close(): Future[Unit] = {
    underlying.close()
  }
}
