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
package com.metamx.tranquility.druid

import io.druid.data.input.impl.{DimensionsSpec, SpatialDimensionSchema}
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.AggregatorFactory
import scala.collection.JavaConverters._

// Not a case class because equality is not well-defined for AggregatorFactories and QueryGranularities.
class DruidRollup(
  val dimensions: DruidDimensions,
  val aggregators: IndexedSeq[AggregatorFactory],
  val indexGranularity: QueryGranularity
)
{
  validate()

  def validate() {
    val dimensionNames = if (dimensions.spec.hasCustomDimensions) dimensions.spec.getDimensions.asScala else Nil
    val spatialDimensionNames = dimensions.spec.getSpatialDimensions.asScala.map(_.getDimName)
    val metricNames = aggregators.map(_.getName)

    val allColumnNames = Seq(DruidRollup.InternalTimeColumnName) ++
      dimensionNames ++
      spatialDimensionNames ++
      metricNames

    val duplicateColumns = allColumnNames.groupBy(identity).filter(_._2.size > 1).keySet

    if (duplicateColumns.nonEmpty) {
      throw new IllegalArgumentException("Duplicate columns: %s" format duplicateColumns.mkString(", "))
    }
  }
}

sealed abstract class DruidDimensions
{
  def spec: DimensionsSpec

  def spatialDimensions: IndexedSeq[DruidSpatialDimension]

  def withSpatialDimensions(xs: java.util.List[DruidSpatialDimension]): DruidDimensions
}

sealed abstract class DruidSpatialDimension
{
  def schema: SpatialDimensionSchema
}

case class SingleFieldDruidSpatialDimension(name: String) extends DruidSpatialDimension
{
  override def schema = new SpatialDimensionSchema(name, List.empty[String].asJava)
}

case class MultipleFieldDruidSpatialDimension(name: String, fieldNames: Seq[String]) extends DruidSpatialDimension
{
  override def schema = new SpatialDimensionSchema(name, fieldNames.asJava)
}

case class SpecificDruidDimensions(
  dimensions: IndexedSeq[String],
  spatialDimensions: IndexedSeq[DruidSpatialDimension] = Vector.empty
) extends DruidDimensions
{
  override def spec = {
    // Sort dimenions as a workaround for https://github.com/metamx/druid/issues/658
    // (Indexer does not merge properly when dimensions are provided in non-lexicographic order.)
    new DimensionsSpec(
      dimensions.sorted.asJava,
      null,
      spatialDimensions.map(_.schema).asJava
    )
  }

  /**
   * Convenience method for Java users. Scala users should use "copy".
   */
  override def withSpatialDimensions(xs: java.util.List[DruidSpatialDimension]) = copy(
    spatialDimensions = xs
      .asScala
      .toIndexedSeq
  )
}

case class SchemalessDruidDimensions(
  dimensionExclusions: IndexedSeq[String],
  spatialDimensions: IndexedSeq[DruidSpatialDimension] = Vector.empty
) extends DruidDimensions
{
  override def spec = {
    // Null dimensions causes the Druid parser to go schemaless.
    new DimensionsSpec(
      null,
      dimensionExclusions.asJava,
      spatialDimensions.map(_.schema).asJava
    )
  }

  /**
   * Convenience method for Java users. Scala users should use "copy".
   */
  override def withSpatialDimensions(xs: java.util.List[DruidSpatialDimension]) = copy(
    spatialDimensions = xs
      .asScala
      .toIndexedSeq
  )
}

object DruidRollup
{
  private val InternalTimeColumnName = "__time"

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
      SpecificDruidDimensions(dimensions.asScala.toIndexedSeq, Vector.empty),
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
    SpecificDruidDimensions(dimensions.asScala.toIndexedSeq, Vector.empty)
  }

  /**
   * Builder for Java users.
   */
  def schemaless(): DruidDimensions = {
    SchemalessDruidDimensions(Vector.empty, Vector.empty)
  }

  /**
   * Builder for Java users.
   */
  def schemalessWithExclusions(dimensionExclusions: java.util.List[String]): DruidDimensions = {
    SchemalessDruidDimensions(dimensionExclusions.asScala.toIndexedSeq, Vector.empty)
  }
}

object DruidSpatialDimension
{
  /**
   * Builder for Java users.
   */
  def singleField(name: String): DruidSpatialDimension = {
    new SingleFieldDruidSpatialDimension(name)
  }

  /**
   * Builder for Java users.
   */
  def multipleField(name: String, fieldNames: java.util.List[String]): DruidSpatialDimension = {
    new MultipleFieldDruidSpatialDimension(name, fieldNames.asScala)
  }
}
