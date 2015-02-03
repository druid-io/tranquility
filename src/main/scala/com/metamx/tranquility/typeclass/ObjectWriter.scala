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
