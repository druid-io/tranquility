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

import com.metamx.tranquility.beam.Beam
import org.apache.samza.config.Config
import org.apache.samza.system.SystemStream

/**
 * Implement this class to link up Samza with Tranquility.
 */
abstract class BeamFactory
{
  /**
   * Create a Beam given a particular Samza SystemStream and Config. The Config is not subsetted; it's the config for
   * the entire job.
   *
   * @param stream stream for this beam
   * @param config config for this job
   * @return beam for a BeamProducer
   */
  def makeBeam(stream: SystemStream, config: Config): Beam[Any]
}
