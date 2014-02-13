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
