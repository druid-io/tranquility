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
