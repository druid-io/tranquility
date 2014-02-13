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

import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.AggregatorFactory
import scala.collection.JavaConverters._

// Not a case class because equality is not well-defined for AggregatorFactories and QueryGranularities.
class DruidRollup(
  val dimensions: IndexedSeq[String],
  val aggregators: IndexedSeq[AggregatorFactory],
  val indexGranularity: QueryGranularity
  )
{
  /**
   * Combining aggregators know how to combine metrics from two beta (already-rolled-up) events. Contrast this with
   * regular aggregators, which know how turn alpha (non-rolled-up) events into beta events.
   */
  def combiningAggregators: IndexedSeq[AggregatorFactory] = aggregators.map(_.getCombiningFactory)
}

object DruidRollup
{
  val DefaultTimestampColumn = "timestamp"
  val DefaultTimestampFormat = "iso"

  /**
   * Builder for Scala users.
   */
  def apply(
    dimensions: Seq[String],
    aggregators: Seq[AggregatorFactory],
    indexGranularity: QueryGranularity
  ) =
  {
    new DruidRollup(dimensions.toIndexedSeq, aggregators.toIndexedSeq, indexGranularity)
  }

  /**
   * Builder for Java users.
   */
  def create(
    dimensions: java.util.List[String],
    aggregators: java.util.List[AggregatorFactory],
    indexGranularity: QueryGranularity
  ) =
  {
    new DruidRollup(dimensions.asScala.toIndexedSeq, aggregators.asScala.toIndexedSeq, indexGranularity)
  }
}
