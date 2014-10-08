package com.metamx.tranquility.typeclass

/**
 * Like ObjectWriter, but easier to implement from within Java code.
 */
trait JavaObjectWriter[A]
{
  /**
   * Serialize a single object. When serializing to JSON, this should result in a JSON object.
   */
  def asBytes(obj: A): Array[Byte]

  /**
   * Serialize a batch of objects to send all at once. When serializing to JSON, this should result in a JSON array
   * of objects.
   */
  def batchAsBytes(objects: java.util.Iterator[A]): Array[Byte]
}
