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

package com.metamx.tranquility.test.common

import com.metamx.common.scala.Logging
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.SendResult
import com.twitter.util.Future
import com.twitter.util.Promise
import scala.collection.JavaConverters._
import io.druid.data.input.InputRow
import scala.collection.mutable.ArrayBuffer

class FailableBeam[A](beam: Beam[A], isFail: A => Boolean, isSuperfail: A => Boolean, isDrop: A => Boolean)
  extends Beam[A] with Logging
{
  override def sendAll(messages: Seq[A]): Seq[Future[SendResult]] = {
    val messagesWithPromises = Vector() ++ messages.map(message => (message, Promise[SendResult]()))
    val sendable = ArrayBuffer[(A, Promise[SendResult])]()

    if (messages.exists(isSuperfail)) {
      throw new IllegalStateException("superfail!")
    }

    for ((message, promise) <- messagesWithPromises) {
      if (isFail(message)) {
        promise.setException(new IllegalStateException("fail!"))
      } else if (isDrop(message)) {
        promise.setValue(SendResult.Dropped)
      } else {
        sendable += ((message, promise))
      }
    }

    // Send sendable messages, and assign their promises.
    for (((message, promise), future) <- sendable zip beam.sendAll(sendable.map(_._1))) {
      promise.become(future)
    }

    messagesWithPromises.map(_._2)
  }

  override def close(): Future[Unit] = beam.close()

  override def toString: String = s"Failable($beam)"
}

object FailableBeam
{
  val ActionKey = "action"
  val Fail = ActionKey -> "__fail__"
  val Superfail = ActionKey -> "__superfail__"
  val Drop = ActionKey -> "__drop__"

  def forStrings(beam: Beam[String]): FailableBeam[String] = new FailableBeam[String](
    beam,
    _ == Fail._2,
    _ == Superfail._2,
    _ == Drop._2
  )

  def forDicts(beam: Beam[Dict]): FailableBeam[Dict] = new FailableBeam[Dict](
    beam,
    _.get(ActionKey) == Some(Fail._2),
    _.get(ActionKey) == Some(Superfail._2),
    _.get(ActionKey) == Some(Drop._2)
  )

  def forInputRows(beam: Beam[InputRow]): FailableBeam[InputRow] = new FailableBeam[InputRow](
    beam,
    _.getDimension(ActionKey).asScala == Seq(Fail._2),
    _.getDimension(ActionKey).asScala == Seq(Superfail._2),
    _.getDimension(ActionKey).asScala == Seq(Drop._2)
  )
}
