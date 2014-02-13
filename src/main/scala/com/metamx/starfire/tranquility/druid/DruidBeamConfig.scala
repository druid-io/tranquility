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
package com.metamx.starfire.tranquility.druid

import org.scala_tools.time.Imports._

trait DruidBeamConfig extends IndexServiceConfig
{
  /**
   * Druid indexing tasks will shut down this long after the windowPeriod has elapsed. The purpose of this extra delay
   * is to allow time to receive the last few events that are valid from our perspective. Otherwise, we could think
   * an event is just barely on-time, but the index task may not be available to receive it.
   */
  def firehoseGracePeriod: Period

  /**
   * When we create new Druid indexing tasks, wait this long for the task to appear before complaining that it cannot
   * be found.
   */
  def firehoseQuietPeriod: Period

  /**
   * If a push to Druid fails for some apparently-transient reason, retry for this long before complaining that the
   * events could not be pushed.
   */
  def firehoseRetryPeriod: Period

  /**
   * Maximum number of events to send to Druid in one HTTP request. Larger batches will be broken up.
   */
  def firehoseChunkSize: Int
}
