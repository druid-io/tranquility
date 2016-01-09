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
package com.metamx.tranquility.beam

import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.typeclass.JsonWriter
import com.twitter.util.Future
import scala.collection.immutable.BitSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MemoryBeam[A](
  val key: String,
  jsonWriter: JsonWriter[A]
) extends Beam[A]
{
  override def sendBatch(events: Seq[A]): Future[BitSet] = {
    events.map(event => Jackson.parse[Dict](jsonWriter.asBytes(event))) foreach {
      d =>
        MemoryBeam.add(key, d)
    }
    Future.value(BitSet.empty ++ events.indices)
  }

  override def close() = Future.Done

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
