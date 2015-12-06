/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.metamx.tranquility.beam

import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import org.joda.time.{DateTime, Period}

case class ClusteredBeamTuning(
  segmentGranularity: Granularity = Granularity.HOUR,
  warmingPeriod: Period = new Period(0),
  windowPeriod: Period = new Period("PT10M"),
  partitions: Int = 1,
  replicants: Int = 1,
  minSegmentsPerBeam: Int = 1,
  maxSegmentsPerBeam: Int = 1
) extends Logging
{
  if (maxSegmentsPerBeam != 1 || minSegmentsPerBeam != 1) {
    log.warn(
      "minSegmentsPerBeam and maxSegmentsPerBeam are experimental settings; their behavior may change "
        + "or they may be removed without warning. Be careful!"
    )
  }

  def segmentBucket(ts: DateTime) = segmentGranularity.bucket(ts)
}

object ClusteredBeamTuning
{

  /**
    * Builder for ClusteredBeamTuning objects.
    */
  def builder() = new Builder(ClusteredBeamTuning())

  class Builder private[tranquility](config: ClusteredBeamTuning)
  {
    /**
      * Each sub-beam will cover blocks of this size in the timeline. This controls how often segments are closed off
      * and made immutable.
      *
      * Default is Granularity.HOUR.
      */
    def segmentGranularity(x: Granularity) = new Builder(config.copy(segmentGranularity = x))

    /**
      * If nonzero, create sub-beams early. This can be useful if sub-beams take a long time to start up.
      *
      * Default is PT0M (off).
      */
    def warmingPeriod(x: Period) = new Builder(config.copy(warmingPeriod = x))

    /**
      * Accept events this far outside of their timeline block. For example, if it's currently 1:25PM, and your
      * windowPeriod is PT10M, and your segmentGranularity is HOUR, Tranquility will accept events timestamped anywhere
      * from 12:50PM to 2:10PM (but will drop events outside that range).
      *
      * Default is PT10M.
      */
    def windowPeriod(x: Period) = new Builder(config.copy(windowPeriod = x))

    /**
      * Create this many logically distinct sub-beams per timeline block. This is used to scale ingestion up to
      * handle larger streams.
      *
      * Default is 1.
      */
    def partitions(x: Int) = new Builder(config.copy(partitions = x))

    /**
      * Create this many replicants per sub-beam. This is used to provide higher availability and parallelism for
      * queries.
      *
      * Default is 1.
      */
    def replicants(x: Int) = new Builder(config.copy(replicants = x))

    /**
      * Create beams that cover at least this many segments (randomly between minSegmentsPerBeam and maxSegmentsPerBeam).
      * This can be useful if you want to minimize beam turnover.
      *
      * Default is 1.
      *
      * Warning: This is an experimental setting, and may be removed without further warning.
      */
    def minSegmentsPerBeam(x: Int) = new Builder(config.copy(minSegmentsPerBeam = x))

    /**
      * Create beams that cover at most this many segments (randomly between minSegmentsPerBeam and maxSegmentsPerBeam).
      * This can be useful if you want to minimize beam turnover.
      *
      * Default is 1.
      *
      * Warning: This is an experimental setting, and may be removed without further warning.
      */
    def maxSegmentsPerBeam(x: Int) = new Builder(config.copy(maxSegmentsPerBeam = x))

    def build(): ClusteredBeamTuning = config
  }

  /**
    * Factory method for ClusteredBeamTuning objects.
    */
  @deprecated("use ClusteredBeamTuning.builder()", "0.2.26")
  def create(
    segmentGranularity: Granularity,
    warmingPeriod: Period,
    windowPeriod: Period,
    partitions: Int,
    replicants: Int
  ): ClusteredBeamTuning =
  {
    apply(segmentGranularity, warmingPeriod, windowPeriod, partitions, replicants, 1, 1)
  }
}
