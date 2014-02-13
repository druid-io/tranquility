package com.metamx.starfire.tranquility.finagle

import com.metamx.starfire.tranquility.beam.Beam
import com.twitter.finagle.Service
import com.twitter.util.Time

/**
 * Bridges Beams with Finagle Services.
 */
class BeamService[A](beam: Beam[A]) extends Service[Seq[A], Int]
{
  def apply(request: Seq[A]) = beam.propagate(request)

  override def close(deadline: Time) = beam.close()
}
