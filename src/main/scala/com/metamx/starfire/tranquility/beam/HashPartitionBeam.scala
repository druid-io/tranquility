package com.metamx.starfire.tranquility.beam

import com.metamx.common.scala.Logging
import com.twitter.util.Future

/**
 * Partitions events based on their hashCode modulo the number of delegate beams, and propagates the partitioned events
 * via the appropriate beam.
 */
class HashPartitionBeam[A](
  val delegates: IndexedSeq[Beam[A]]
) extends Beam[A] with Logging
{
  def propagate(events: Seq[A]) = {
    val futures = events.groupBy(event => math.abs(event.hashCode % delegates.size)) map {
      case (i, group) =>
        delegates(i).propagate(group)
    }
    Future.collect(futures.toList).map(_.sum)
  }

  def close() = {
    Future.collect(delegates map (_.close())) map (_ => ())
  }

  override def toString = "HashPartitionBeam(%s)" format delegates.mkString(", ")
}
