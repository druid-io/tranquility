package com.metamx.starfire.tranquility.storm

import backtype.storm.task.IMetricsContext
import com.metamx.starfire.tranquility.beam.Beam
import java.{util => ju}

trait BeamFactory[EventType] extends Serializable
{
  def makeBeam(conf: ju.Map[_, _], metrics: IMetricsContext): Beam[EventType]
}
