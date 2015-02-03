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
package com.metamx.tranquility.samza

import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.Beam
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin

class BeamSystemFactory[A, BeamType <: Beam[A]] extends SystemFactory with Logging
{
  override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new UnsupportedOperationException("Can't consume from a beam!")
  }

  override def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val beamFactoryClass = config.getClass("systems.%s.beam.factory" format systemName)
    val batchSize = config.getInt("systems.%s.beam.batchSize" format systemName, 2000)
    val maxPendingBatches = config.getInt("systems.%s.beam.maxPendingBatches" format systemName, 5)
    val throwOnError = config.getBoolean("systems.%s.beam.throwOnError" format systemName, true)

    log.info(
      "Creating BeamProducer for system[%s] with beamFactory[%s], batchSize[%,d], maxPendingBatches[%,d].",
      systemName,
      beamFactoryClass,
      batchSize,
      maxPendingBatches
    )

    val beamFactory: BeamFactory = beamFactoryClass.newInstance()
    new BeamProducer(beamFactory, systemName, config, batchSize, maxPendingBatches, throwOnError)
  }

  override def getAdmin(systemName: String, config: Config) = {
    new SinglePartitionWithoutOffsetsSystemAdmin
  }
}
