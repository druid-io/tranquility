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
package com.metamx.tranquility.druid

import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.AggregatorFactory
import scala.collection.JavaConverters._

// Not a case class because equality is not well-defined for AggregatorFactories and QueryGranularities.
class DruidRollup(
  val dimensions: DruidDimensions,
  val aggregators: IndexedSeq[AggregatorFactory],
  val indexGranularity: QueryGranularity
)

sealed abstract class DruidDimensions
case class SpecificDruidDimensions(dimensions: IndexedSeq[String]) extends DruidDimensions
case class SchemalessDruidDimensions(dimensionExclusions: IndexedSeq[String]) extends DruidDimensions

object DruidRollup
{
  /**
   * Builder for Scala users. Accepts a druid dimensions object and can be used to build rollups based on specific
   * or schemaless dimensions.
   */
  def apply(
    dimensions: DruidDimensions,
    aggregators: Seq[AggregatorFactory],
    indexGranularity: QueryGranularity
  ) =
  {
    new DruidRollup(dimensions, aggregators.toIndexedSeq, indexGranularity)
  }

  /**
   * Builder for Java users. Accepts a druid dimensions object and can be used to build rollups based on specific
   * or schemaless dimensions.
   */
  def create(
    dimensions: DruidDimensions,
    aggregators: java.util.List[AggregatorFactory],
    indexGranularity: QueryGranularity
  ) =
  {
    new DruidRollup(
      dimensions,
      aggregators.asScala.toIndexedSeq,
      indexGranularity
    )
  }

  /**
   * Builder for Java users. Accepts dimensions as strings, and creates a rollup with those specific dimensions.
   */
  def create(
    dimensions: java.util.List[String],
    aggregators: java.util.List[AggregatorFactory],
    indexGranularity: QueryGranularity
  ) =
  {
    new DruidRollup(
      SpecificDruidDimensions(dimensions.asScala.toIndexedSeq),
      aggregators.asScala.toIndexedSeq,
      indexGranularity
    )
  }
}

object DruidDimensions
{
  /**
   * Builder for Java users.
   */
  def specific(dimensions: java.util.List[String]): DruidDimensions = {
    SpecificDruidDimensions(dimensions.asScala.toIndexedSeq)
  }

  /**
   * Builder for Java users.
   */
  def schemaless(): DruidDimensions = {
    SchemalessDruidDimensions(Vector.empty)
  }

  /**
   * Builder for Java users.
   */
  def schemalessWithExclusions(dimensionExclusions: java.util.List[String]): DruidDimensions = {
    SchemalessDruidDimensions(dimensionExclusions.asScala.toIndexedSeq)
  }
}
