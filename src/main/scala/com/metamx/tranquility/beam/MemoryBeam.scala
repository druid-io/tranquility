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

import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.typeclass.JsonWriter
import com.twitter.util.Future
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MemoryBeam[A](
  key: String,
  jsonWriter: JsonWriter[A]
) extends Beam[A]
{
  def propagate(events: Seq[A]) = {
    events.map(event => Jackson.parse[Dict](jsonWriter.asBytes(event))) foreach {
      d =>
        MemoryBeam.add(key, d)
    }
    Future.value(events.size)
  }

  def close() = Future.Done

  override def toString = "MemoryBeam(key = %s)" format key
}

object MemoryBeam
{
  private val buffers = mutable.HashMap[String, ArrayBuffer[Dict]]()

  def add(key: String, value: Dict) {
    buffers.synchronized {
      buffers.getOrElseUpdate(key, ArrayBuffer()) += value
    }
  }

  def get(): Map[String, IndexedSeq[Dict]] = {
    buffers.synchronized {
      buffers.mapValues(_.toIndexedSeq).toMap.map(identity)
    }
  }

  def get(key: String): Seq[Dict] = get()(key)

  def clear() {
    buffers.synchronized {
      buffers.clear()
    }
  }
}
