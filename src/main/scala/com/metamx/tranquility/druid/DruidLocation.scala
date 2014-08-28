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

case class DruidLocation(
  environment: DruidEnvironment,
  dataSource: String
)

object DruidLocation
{
  def apply(
    indexServiceMaybeWithSlashes: String,
    firehoseServicePattern: String,
    dataSource: String
  ): DruidLocation =
  {
    create(indexServiceMaybeWithSlashes, firehoseServicePattern, dataSource)
  }

  /**
   * Factory method for creating DruidLocation objects. DruidLocations represent a specific Druid dataSource in a
   * specific Druid indexing service cluster.
   *
   * @param environment the Druid indexing service
   * @param dataSource the Druid dataSource
   */
  def create(environment: DruidEnvironment, dataSource: String): DruidLocation = {
    DruidLocation(environment, dataSource)
  }

  /**
   * Factory method for creating DruidLocation objects. DruidLocations represent a specific Druid dataSource in a
   * specific Druid indexing service cluster.
   *
   * @param indexServiceMaybeWithSlashes Your overlord's "druid.service" configuration. Slashes will be replaced with
   *                                     colons before searching for this in service discovery, because Druid does the
   *                                     same thing before announcing.
   * @param firehoseServicePattern Make up a service pattern, include %s somewhere in it. This will be used for
   *                               internal service-discovery purposes, to help Tranquility find Druid indexing tasks.
   * @param dataSource the Druid dataSource
   */
  def create(
    indexServiceMaybeWithSlashes: String,
    firehoseServicePattern: String,
    dataSource: String
  ): DruidLocation =
  {
    DruidLocation(DruidEnvironment(indexServiceMaybeWithSlashes, firehoseServicePattern), dataSource)
  }
}
