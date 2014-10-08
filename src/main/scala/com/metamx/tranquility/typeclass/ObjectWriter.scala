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
package com.metamx.tranquility.typeclass

import scala.collection.JavaConverters._

/**
 * Serializes objects for beaming out to other services (such as Druid).
 */
trait ObjectWriter[A] extends Serializable
{
  /**
   * Serialize a single object. When serializing to JSON, this should result in a JSON object.
   */
  def asBytes(obj: A): Array[Byte]

  /**
   * Serialize a batch of objects to send all at once. When serializing to JSON, this should result in a JSON array
   * of objects.
   */
  def batchAsBytes(objects: TraversableOnce[A]): Array[Byte]
}

object ObjectWriter
{
  def wrap[A](javaObjectWriter: JavaObjectWriter[A]): ObjectWriter[A] = new ObjectWriter[A] {
    override def asBytes(obj: A) = javaObjectWriter.asBytes(obj)
    override def batchAsBytes(objects: TraversableOnce[A]) = javaObjectWriter.batchAsBytes(objects.toIterator.asJava)
  }
}
