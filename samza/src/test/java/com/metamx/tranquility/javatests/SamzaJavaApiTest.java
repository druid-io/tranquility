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

import com.google.common.collect.ImmutableMap;
import com.metamx.tranquility.samza.BeamProducer;
import com.metamx.tranquility.samza.BeamSystemFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;

public class SamzaJavaApiTest
{
  @Test
  public void testDruidSystemFactory() throws Exception
  {
    final Config config = new MapConfig(
        ImmutableMap.<String, String>builder()
                    .put("systems.foo.samza.factory", "com.metamx.tranquility.samza.BeamSystemFactory")
                    .put("systems.foo.beam.factory", "com.metamx.tranquility.javatests.TestSamzaBeamFactory")
                    .build()
    );

    final SystemFactory systemFactory = new BeamSystemFactory();
    final SystemProducer producer = systemFactory.getProducer("foo", config, new NoOpMetricsRegistry());
    Assert.assertTrue(producer instanceof BeamProducer);
  }
}
