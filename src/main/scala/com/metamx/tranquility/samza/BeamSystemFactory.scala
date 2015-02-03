/*
 * Tranquility.
 * Copyright (C) 2013, 2014, 2015  Metamarkets Group Inc.
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
