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

import com.google.common.hash.Hashing
import com.metamx.common.scala.Logging
import com.twitter.util.Future

/**
 * Partitions events based on their hashCode modulo the number of delegate beams, and propagates the partitioned events
 * via the appropriate beam.
 */
class HashPartitionBeam[A](
  val delegates: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  def propagate(events: Seq[A]) = {
    val futures = events.groupBy(event => Hashing.consistentHash(event.hashCode, delegates.size)) map {
      case (i, group) =>
        delegates(i).propagate(group)
    }
    Future.collect(futures.toList).map(_.sum)
  }

  def close() = {
    Future.collect(delegates map (_.close())) map (_ => ())
  }

  override def toString = "HashPartitionBeam(%s)" format delegates.mkString(", ")
}
