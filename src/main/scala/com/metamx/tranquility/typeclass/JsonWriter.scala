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

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import com.metamx.common.scala.Predef._
import java.io.ByteArrayOutputStream

trait JsonWriter[A] extends ObjectWriter[A]
{
  @transient private lazy val _jsonFactory = new JsonFactory

  override def asBytes(a: A): Array[Byte] = {
    val out = new ByteArrayOutputStream
    _jsonFactory.createGenerator(out).withFinally(_.close) {
      jg =>
        viaJsonGenerator(a, jg)
    }
    out.toByteArray
  }

  override def batchAsBytes(as: TraversableOnce[A]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    _jsonFactory.createGenerator(out).withFinally(_.close) {
      jg =>
        jg.writeStartArray()
        as foreach (viaJsonGenerator(_, jg))
        jg.writeEndArray()
    }
    out.toByteArray
  }

  protected def viaJsonGenerator(a: A, jg: JsonGenerator)
}
