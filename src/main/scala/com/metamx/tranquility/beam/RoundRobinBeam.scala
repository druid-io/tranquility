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

import com.metamx.common.scala.Logging
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicInteger

/**
 * Farms out events to various beams, round-robin.
 */
class RoundRobinBeam[A](
  beams: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  private[this] val n = new AtomicInteger(-1)

  def propagate(events: Seq[A]) = {
    beams(n.incrementAndGet() % beams.size).propagate(events)
  }

  def close() = {
    Future.collect(beams map (_.close())) map (_ => ())
  }

  override def toString = "RoundRobinBeam(%s)" format beams.mkString(", ")
}
