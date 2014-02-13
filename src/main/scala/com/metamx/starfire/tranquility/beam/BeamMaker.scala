package com.metamx.starfire.tranquility.beam

import com.metamx.common.scala.untyped._
import org.scala_tools.time.Imports._

/**
 * Makes beams for particular intervals and partition numbers. Can also serialize and deserialize beam representations.
 *
 * @tparam A event type
 * @tparam BeamType specific beam type we know how to create
 */
trait BeamMaker[A, BeamType <: Beam[A]]
{
  def newBeam(interval: Interval, partition: Int): BeamType

  def toDict(beam: BeamType): Dict

  def fromDict(d: Dict): BeamType

  def intervalFromDict(d: Dict): Interval

  def partitionFromDict(d: Dict): Int
}
