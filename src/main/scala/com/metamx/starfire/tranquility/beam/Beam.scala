package com.metamx.starfire.tranquility.beam

import com.twitter.util.Future

/**
 * Beams can accept events and forward them along. The propagate method may throw a DefunctBeamException, which means
 * the beam should be discarded (after calling close()).
 */
trait Beam[A]
{
  /**
   * Request propagation of events. The operation may fail in various ways, which tend to be specific to
   * the implementing class. Returns the number of events propagated.
   */
  def propagate(events: Seq[A]): Future[Int]

  /**
   * Signal that no more events will be sent. Many implementations need this to do proper cleanup.
   */
  def close(): Future[Unit]
}

class DefunctBeamException(s: String, t: Throwable) extends Exception(s, t)
{
  def this(s: String) = this(s, null)
}
