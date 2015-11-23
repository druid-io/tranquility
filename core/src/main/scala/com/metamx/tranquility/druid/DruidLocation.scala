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

  /**
    * Factory method for creating DruidLocation objects. DruidLocations represent a specific Druid dataSource in a
    * specific Druid indexing service cluster.
    *
    * @param indexServiceMaybeWithSlashes Your overlord's "druid.service" configuration. Slashes will be replaced with
    *                                     colons before searching for this in service discovery, because Druid does the
    *                                     same thing before announcing.
    * @param dataSource the Druid dataSource
    */
  def create(
    indexServiceMaybeWithSlashes: String,
    dataSource: String
  ): DruidLocation =
  {
    DruidLocation(DruidEnvironment(indexServiceMaybeWithSlashes), dataSource)
  }
}
