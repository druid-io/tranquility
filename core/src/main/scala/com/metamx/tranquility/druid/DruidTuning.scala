/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.tranquility.druid

import com.metamx.common.scala.untyped.Dict
import org.joda.time.Period
import org.scala_tools.time.Imports._

case class DruidTuning(
  maxRowsInMemory: Int = 75000,
  intermediatePersistPeriod: Period = 10.minutes,
  maxPendingPersists: Int = 0,
  buildV9Directly: Boolean = false
)
{
  def toMap: Dict = Dict(
    "type" -> "realtime",
    "maxRowsInMemory" -> maxRowsInMemory,
    "intermediatePersistPeriod" -> intermediatePersistPeriod.toString(),
    "maxPendingPersists" -> maxPendingPersists,
    "buildV9Directly" -> buildV9Directly
  )
}

object DruidTuning
{
  /**
    * Real-time Druid tuning parameters. These are passed directly to the Druid indexing service. See the Druid
    * documentation for their meanings.
    *
    * @param maxRowsInMemory           number of rows to aggregate before persisting
    * @param intermediatePersistPeriod period that determines the rate at which intermediate persists occur
    * @param maxPendingPersists        number of persists that can be pending, but not started
    */
  @deprecated("use 'apply' or 'builder'", "0.7.3")
  def create(
    maxRowsInMemory: Int,
    intermediatePersistPeriod: Period,
    maxPendingPersists: Int
  ): DruidTuning =
  {
    apply(maxRowsInMemory, intermediatePersistPeriod, maxPendingPersists)
  }

  /**
    * Builder for DruidTuning objects.
    */
  def builder() = new Builder(DruidTuning())

  /**
    * For internal use in DruidBeams.
    */
  private[tranquility] def fromMap(d: Dict): DruidTuning = {
    val defaults = DruidTuning()
    DruidTuning(
      maxRowsInMemory = d.get("maxRowsInMemory").map(String.valueOf(_).toInt).getOrElse(defaults.maxRowsInMemory),
      intermediatePersistPeriod = d.get("intermediatePersistPeriod").map(new Period(_)).getOrElse(
        defaults.intermediatePersistPeriod
      ),
      maxPendingPersists = d.get("maxPendingPersists").map(String.valueOf(_).toInt)
        .getOrElse(defaults.maxPendingPersists),
      buildV9Directly = d.get("buildV9Directly").map(String.valueOf(_).toBoolean).getOrElse(defaults.buildV9Directly)
    )
  }

  class Builder private[tranquility](config: DruidTuning)
  {
    /**
      * Number of rows to aggregate before persisting.
      *
      * Default is 75000.
      */
    def maxRowsInMemory(x: Int) = new Builder(config.copy(maxRowsInMemory = x))

    /**
      * Period that determines the rate at which intermediate persists occur.
      *
      * Default is 10 minutes.
      */
    def intermediatePersistPeriod(x: Period) = new Builder(config.copy(intermediatePersistPeriod = x))

    /**
      * Number of persists that can be pending, but not started.
      *
      * Default is 0.
      */
    def maxPendingPersists(x: Int) = new Builder(config.copy(maxPendingPersists = x))

    /**
      * Should Druid be asked to build v9 segments directly? Only supported in certain versions of Druid.
      * See your Druid version's documentation for details.
      *
      * Default is false.
      */
    def buildV9Directly(x: Boolean) = new Builder(config.copy(buildV9Directly = x))

    def build(): DruidTuning = config
  }

}
