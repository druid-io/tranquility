package com.metamx.starfire.tranquility.beam

import com.twitter.util.Future

class NoopBeam[A] extends Beam[A]
{
  def propagate(events: Seq[A]) = Future.value(0)

  def close() = Future.Done

  override def toString = "NoopBeam()"
}
