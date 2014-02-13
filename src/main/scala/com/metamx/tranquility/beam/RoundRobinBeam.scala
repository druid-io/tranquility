/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
