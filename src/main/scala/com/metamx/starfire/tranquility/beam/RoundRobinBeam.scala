package com.metamx.starfire.tranquility.beam

import com.metamx.common.scala.Logging
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicInteger

/**
 * Farms out events to various beams, round-robin.
 */
class RoundRobinBeam[A](
  beams: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  private[this] val n = new AtomicInteger(-1)

  def propagate(events: Seq[A]) = {
    beams(n.incrementAndGet() % beams.size).propagate(events)
  }

  def close() = {
    Future.collect(beams map (_.close())) map (_ => ())
  }

  override def toString = "RoundRobinBeam(%s)" format beams.mkString(", ")
}
