package com.metamx.starfire.tranquility.typeclass

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
