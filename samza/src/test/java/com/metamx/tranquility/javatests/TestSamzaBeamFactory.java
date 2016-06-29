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

package com.metamx.tranquility.javatests;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.samza.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

public class TestSamzaBeamFactory implements BeamFactory
{

  @Override
  public Beam<Object> makeBeam(SystemStream stream, Config config)
  {
    final String zkConnect = "localhost";
    final String dataSource = stream.getStream();

    final List<String> dimensions = ImmutableList.of("column");
    final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
        new CountAggregatorFactory("cnt")
    );

    // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
    // Objects, so you'll generally have to cast them here.
    final Timestamper<Object> timestamper = new Timestamper<Object>()
    {
      @Override
      public DateTime timestamp(Object obj)
      {
        final Map<String, Object> theMap = (Map<String, Object>) obj;
        return new DateTime(theMap.get("timestamp"));
      }
    };

    final CuratorFramework curator = CuratorFrameworkFactory.builder()
                                                            .connectString(zkConnect)
                                                            .retryPolicy(new ExponentialBackoffRetry(500, 15, 10000))
                                                            .build();
    curator.start();

    return DruidBeams
        .builder(timestamper)
        .curator(curator)
        .discoveryPath("/druid/discovery")
        .location(DruidLocation.create("overlord", "druid:firehose:%s", dataSource))
        .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularities.MINUTE))
        .tuning(
            ClusteredBeamTuning.builder()
                               .segmentGranularity(Granularity.HOUR)
                               .windowPeriod(new Period("PT10M"))
                               .build()
        )
        .buildBeam();
  }
}
