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
  segmentGranularity: Granularity,
  warmingPeriod: Period,
  windowPeriod: Period,
  partitions: Int,
  replicants: Int
)
{
  def segmentBucket(ts: DateTime) = segmentGranularity.bucket(ts)
}

object ClusteredBeamTuning
{
  /**
   * Factory method for ClusteredBeamTuning objects.
   *
   * @param segmentGranularity Each sub-beam will cover blocks of this size in the timeline. This controls how often
   *                           segments are closed off and made immutable. {{{Granularity.HOUR}}} is usually reasonable.
   * @param warmingPeriod If nonzero, create sub-beams this early. This can be useful if sub-beams take a long time
   *                      to start up.
   * @param windowPeriod Accept events this far outside of their timeline block. For example, if it's currently
   *                     1:25PM, and your windowPeriod is PT10M, and your segmentGranularity is HOUR, Tranquility will
   *                     accept events timestamped anywhere from 12:50PM to 2:10PM (but will drop events outside
   *                     that range).
   * @param partitions Create this many logically distinct sub-beams per timeline block. This is used to scale
   *                   ingestion up to handle larger streams.
   * @param replicants Create this many replicants per sub-beam. This is used to provide higher availability and
   *                   parallelism for queries.
   */
  def create(
    segmentGranularity: Granularity,
    warmingPeriod: Period,
    windowPeriod: Period,
    partitions: Int,
    replicants: Int
  ): ClusteredBeamTuning = {
    apply(segmentGranularity, warmingPeriod, windowPeriod, partitions, replicants)
  }
}
