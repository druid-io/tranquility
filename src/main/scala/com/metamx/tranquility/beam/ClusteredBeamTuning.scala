/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.tranquility.beam

import com.metamx.common.Granularity
import org.joda.time.{DateTime, Period}

case class ClusteredBeamTuning(
  segmentGranularity: Granularity = Granularity.HOUR,
  warmingPeriod: Period = new Period(0),
  windowPeriod: Period = new Period("PT10M"),
  partitions: Int = 1,
  replicants: Int = 1,
  minSegmentsPerBeam: Int = 1,
  maxSegmentsPerBeam: Int = 1
)
{
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
     * and made immutable. Default: Granularity.HOUR.
     */
    def segmentGranularity(x: Granularity) = new Builder(config.copy(segmentGranularity = x))

    /**
     * If nonzero, create sub-beams early. This can be useful if sub-beams take a long time to start up. Default: 0.
     */
    def warmingPeriod(x: Period) = new Builder(config.copy(warmingPeriod = x))

    /**
     * Accept events this far outside of their timeline block. For example, if it's currently 1:25PM, and your
     * windowPeriod is PT10M, and your segmentGranularity is HOUR, Tranquility will accept events timestamped anywhere
     * from 12:50PM to 2:10PM (but will drop events outside that range). Default: PT10M.
     */
    def windowPeriod(x: Period) = new Builder(config.copy(windowPeriod = x))

    /**
     * Create this many logically distinct sub-beams per timeline block. This is used to scale ingestion up to
     * handle larger streams. Default: 1.
     */
    def partitions(x: Int) = new Builder(config.copy(partitions = x))

    /**
     * Create this many replicants per sub-beam. This is used to provide higher availability and parallelism for
     * queries. Default: 1.
     */
    def replicants(x: Int) = new Builder(config.copy(replicants = x))

    /**
     * Create beams that cover at least this many segments (randomly between minSegmentsPerBeam and maxSegmentsPerBeam).
     * This can be useful if you want to minimize beam turnover. Default: 1.
     *
     * Warning: This is an experimental setting, and may be removed without warning.
     */
    def minSegmentsPerBeam(x: Int) = new Builder(config.copy(minSegmentsPerBeam = x))

    /**
     * Create beams that cover at most this many segments (randomly between minSegmentsPerBeam and maxSegmentsPerBeam).
     * This can be useful if you want to minimize beam turnover. Default: 1.
     *
     * Warning: This is an experimental setting, and may be removed without warning.
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
