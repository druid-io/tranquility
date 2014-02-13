package com.metamx.starfire.tranquility.typeclass

import org.joda.time.DateTime

trait Timestamper[A]
{
  def timestamp(a: A): DateTime
}
