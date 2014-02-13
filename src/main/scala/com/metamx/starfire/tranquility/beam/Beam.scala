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
package com.metamx.starfire.tranquility.beam

import com.twitter.util.Future

/**
 * Beams can accept events and forward them along. The propagate method may throw a DefunctBeamException, which means
 * the beam should be discarded (after calling close()).
 */
trait Beam[A]
{
  /**
   * Request propagation of events. The operation may fail in various ways, which tend to be specific to
   * the implementing class. Returns the number of events propagated.
   */
  def propagate(events: Seq[A]): Future[Int]

  /**
   * Signal that no more events will be sent. Many implementations need this to do proper cleanup.
   */
  def close(): Future[Unit]
}

class DefunctBeamException(s: String, t: Throwable) extends Exception(s, t)
{
  def this(s: String) = this(s, null)
}
