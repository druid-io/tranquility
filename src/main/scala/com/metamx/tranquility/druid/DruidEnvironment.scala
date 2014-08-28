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
