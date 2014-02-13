package com.metamx.starfire.tranquility.typeclass

trait ObjectWriter[A] extends Serializable
{
  def asBytes(a: A): Array[Byte]

  def batchAsBytes(as: TraversableOnce[A]): Array[Byte]
}
