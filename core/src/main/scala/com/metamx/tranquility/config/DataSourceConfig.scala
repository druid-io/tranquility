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

package com.metamx.tranquility.config

import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.untyped.dict
import com.metamx.common.scala.untyped.str
import com.metamx.tranquility.tranquilizer.Tranquilizer

case class DataSourceConfig[T <: PropertiesBasedConfig](
  dataSource: String,
  propertiesBasedConfig: T,
  specMap: Dict
)
{
  validate()

  def tranquilizerBuilder(): Tranquilizer.Builder = {
    Tranquilizer.builder()
      .maxBatchSize(propertiesBasedConfig.tranquilityMaxBatchSize)
      .maxPendingBatches(propertiesBasedConfig.tranquilityMaxPendingBatches)
      .lingerMillis(propertiesBasedConfig.tranquilityLingerMillis)
  }

  private def validate(): Unit = {
    // Sanity check: two ways of providing dataSource, they must match
    val specDataSource = try {
      str(dict(specMap("dataSchema"))("dataSource"))
    }
    catch {
      case e: ClassCastException =>
        throw new IllegalArgumentException(s"spec[$dataSource] is missing 'dataSource' inside 'dataSchema'")
    }
    require(
      dataSource == specDataSource,
      s"dataSource[$dataSource] did not match spec[$specDataSource]"
    )
  }
}
