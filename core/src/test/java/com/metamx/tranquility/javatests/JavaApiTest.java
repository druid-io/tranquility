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

package com.metamx.tranquility.javatests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.tranquility.druid.DruidBeamConfig;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.druid.DruidSpatialDimension;
import com.metamx.tranquility.druid.SchemalessDruidDimensions;
import com.metamx.tranquility.druid.SpecificDruidDimensions;
import com.metamx.tranquility.finagle.FinagleRegistryConfig;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JavaApiTest
{
  private static final List<String> dimensions = ImmutableList.of("column");
  private static final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
      new CountAggregatorFactory(
          "cnt"
      )
  );

  @Test
  public void testSpecificDimensionsRollupConfiguration() throws Exception
  {
    final DruidRollup rollup = DruidRollup.create(
        DruidDimensions.specific(dimensions),
        aggregators,
        QueryGranularity.MINUTE
    );
    Assert.assertTrue(rollup.dimensions() instanceof SpecificDruidDimensions);
    Assert.assertEquals("column", ((SpecificDruidDimensions) rollup.dimensions()).dimensions().iterator().next());
  }

  @Test
  public void testSchemalessDimensionsRollupConfiguration() throws Exception
  {
    final DruidRollup rollup = DruidRollup.create(
        DruidDimensions.schemaless(),
        aggregators,
        QueryGranularity.MINUTE
    );
    Assert.assertTrue(rollup.dimensions() instanceof SchemalessDruidDimensions);
    Assert.assertEquals(0, ((SchemalessDruidDimensions) rollup.dimensions()).dimensionExclusions().size());
  }

  @Test
  public void testSchemalessDimensionsWithExclusionsRollupConfiguration() throws Exception
  {
    final DruidRollup rollup = DruidRollup.create(
        DruidDimensions.schemalessWithExclusions(dimensions),
        aggregators,
        QueryGranularity.MINUTE
    );
    Assert.assertTrue(rollup.dimensions() instanceof SchemalessDruidDimensions);
    Assert.assertEquals("column", ((SchemalessDruidDimensions) rollup.dimensions()).dimensionExclusions().iterator().next());
  }

  @Test
  public void testSchemalessDimensionsWithExclusionsAndSpatialDimensionsRollupConfiguration() throws Exception
  {
    final DruidRollup rollup = DruidRollup.create(
        DruidDimensions.schemalessWithExclusions(dimensions)
                       .withSpatialDimensions(
                           Lists.newArrayList(
                               DruidSpatialDimension.multipleField(
                                   "coord.geo",
                                   Lists.newArrayList("lat", "lon")
                               )
                           )
                       ),
        aggregators,
        QueryGranularity.MINUTE
    );
    Assert.assertTrue(rollup.dimensions() instanceof SchemalessDruidDimensions);
    Assert.assertEquals("column", ((SchemalessDruidDimensions) rollup.dimensions()).dimensionExclusions().iterator().next());
    Assert.assertEquals("coord.geo", rollup.dimensions().spatialDimensions().iterator().next().schema().getDimName());
  }

  @Test
  public void testDruidBeamConfig()
  {
    final DruidBeamConfig druidBeamConfig = DruidBeamConfig.builder()
                                                           .randomizeTaskId(true)
                                                           .firehoseChunkSize(1234)
                                                           .firehoseGracePeriod(new Period(1))
                                                           .firehoseQuietPeriod(new Period(2))
                                                           .firehoseRetryPeriod(new Period(3))
                                                           .indexRetryPeriod(new Period(4))
                                                           .firehoseBufferSize(500)
                                                           .build();

    Assert.assertEquals(1234, druidBeamConfig.firehoseChunkSize());
    Assert.assertEquals(true, druidBeamConfig.randomizeTaskId());
    Assert.assertEquals(new Period(1), druidBeamConfig.firehoseGracePeriod());
    Assert.assertEquals(new Period(2), druidBeamConfig.firehoseQuietPeriod());
    Assert.assertEquals(new Period(3), druidBeamConfig.firehoseRetryPeriod());
    Assert.assertEquals(new Period(4), druidBeamConfig.indexRetryPeriod());
    Assert.assertEquals(500, druidBeamConfig.firehoseBufferSize());
  }

  @Test
  public void testFinagleRegistryConfig()
  {
    final FinagleRegistryConfig finagleRegistryConfig = FinagleRegistryConfig.builder()
                                                                             .finagleEnableFailFast(false)
                                                                             .finagleHttpTimeout(new Period(1))
                                                                             .finagleHttpConnectionsPerHost(1000)
                                                                             .build();

    Assert.assertEquals(false, finagleRegistryConfig.finagleEnableFailFast());
    Assert.assertEquals(new Period(1), finagleRegistryConfig.finagleHttpTimeout());
    Assert.assertEquals(1000, finagleRegistryConfig.finagleHttpConnectionsPerHost());
  }
}
