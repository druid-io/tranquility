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

class DruidEnvironment(
  indexServiceMaybeWithSlashes: String,
  val firehoseServicePattern: String
) extends Equals
{
  // Replace / with : just like Druid does.
  val indexService = indexServiceMaybeWithSlashes.replace('/', ':')

  // Sanity check on firehoseServicePattern.
  require(firehoseServicePattern.contains("%s"), "firehoseServicePattern must contain '%s' somewhere")
  require(!firehoseServicePattern.contains("/"), "firehoseServicePattern must not contain '/' characters")

  def canEqual(other: Any) = other.isInstanceOf[DruidEnvironment]

  override def equals(other: Any) = other match {
    case that: DruidEnvironment =>
      (indexService, firehoseServicePattern) == (that.indexService, that.firehoseServicePattern)
    case _ => false
  }

  override def hashCode = (indexService, firehoseServicePattern).hashCode()
}

object DruidEnvironment
{
  def apply(indexServiceMaybeWithSlashes: String, firehoseServicePattern: String): DruidEnvironment = {
    new DruidEnvironment(indexServiceMaybeWithSlashes, firehoseServicePattern)
  }

  /**
   * Factory method for creating DruidEnvironment objects. DruidEnvironments represent a Druid indexing service
   * cluster, locatable through service discovery.
   *
   * @param indexServiceMaybeWithSlashes Your overlord's "druid.service" configuration. Slashes will be replaced with
   *                                     colons before searching for this in service discovery, because Druid does the
   *                                     same thing before announcing.
   * @param firehoseServicePattern Make up a service pattern, include %s somewhere in it. This will be used for
   *                               internal service-discovery purposes, to help Tranquility find Druid indexing tasks.
   */
  def create(indexServiceMaybeWithSlashes: String, firehoseServicePattern: String): DruidEnvironment = {
    new DruidEnvironment(indexServiceMaybeWithSlashes, firehoseServicePattern)
  }
}
