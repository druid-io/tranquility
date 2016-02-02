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

package com.metamx.tranquility.test

import com.metamx.common.scala.Predef._
import com.metamx.tranquility.druid.DruidGuicer
import io.druid.guice.ExtensionsConfig
import io.druid.query.aggregation.AggregatorFactory
import io.druid.query.aggregation.LongSumAggregatorFactory
import java.util.Properties
import org.scalatest.FunSuite
import org.scalatest.ShouldMatchers
import scala.collection.JavaConverters._

class DruidGuicerTest extends FunSuite with ShouldMatchers
{
  test("Default ExtensionsConfig") {
    val guicer = new DruidGuicer(new Properties)
    val extensionsConfig = guicer.get[ExtensionsConfig]
    extensionsConfig.getCoordinates.asScala should be(Nil)
    extensionsConfig.getRemoteRepositories.asScala should be(
      Seq(
        "https://repo1.maven.org/maven2/",
        "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local"
      )
    )
    extensionsConfig.searchCurrentClassloader should be(true)
  }

  test("Overridden ExtensionsConfig") {
    val guicer = new DruidGuicer(
      new Properties withEffect { props =>
        props.setProperty("druid.extensions.coordinates", "[]")
        props.setProperty("druid.extensions.remoteRepositories", "[\"https://foo.example.com/\"]")
        props.setProperty("druid.extensions.searchCurrentClassloader", "false")
      }
    )
    val extensionsConfig = guicer.get[ExtensionsConfig]
    extensionsConfig.getCoordinates.asScala should be(Nil)
    extensionsConfig.getRemoteRepositories.asScala should be(Seq("https://foo.example.com/"))
    extensionsConfig.searchCurrentClassloader should be(false)
  }

  test("AggregatorFactory serde") {
    val guicer = new DruidGuicer(new Properties)
    val aggregator = guicer.objectMapper.readValue(
      guicer.objectMapper.writeValueAsBytes(new LongSumAggregatorFactory("foo", "bar")),
      classOf[AggregatorFactory]
    )
    aggregator should be(new LongSumAggregatorFactory("foo", "bar"))
  }
}
