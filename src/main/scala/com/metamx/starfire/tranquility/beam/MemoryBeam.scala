package com.metamx.starfire.tranquility.beam

import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.starfire.tranquility.typeclass.JsonWriter
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
