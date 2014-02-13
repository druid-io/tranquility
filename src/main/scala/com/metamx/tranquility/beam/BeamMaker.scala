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

import com.metamx.common.scala.untyped._
import org.scala_tools.time.Imports._

/**
 * Makes beams for particular intervals and partition numbers. Can also serialize and deserialize beam representations.
 *
 * @tparam A event type
 * @tparam BeamType specific beam type we know how to create
 */
trait BeamMaker[A, BeamType <: Beam[A]]
{
  def newBeam(interval: Interval, partition: Int): BeamType

  def toDict(beam: BeamType): Dict

  def fromDict(d: Dict): BeamType

  def intervalFromDict(d: Dict): Interval

  def partitionFromDict(d: Dict): Int
}
