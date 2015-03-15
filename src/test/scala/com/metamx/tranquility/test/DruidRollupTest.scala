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

package com.metamx.tranquility.test

import com.metamx.common.parsers.ParseException
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.SpecificDruidDimensions
import io.druid.granularity.QueryGranularity
import io.druid.query.aggregation.CountAggregatorFactory
import io.druid.query.aggregation.LongSumAggregatorFactory
import org.scalatest.FunSuite
import org.scalatest.MustMatchers

class DruidRollupTest extends FunSuite with MustMatchers
{
  test("Validations: Passing") {
    val rollup = DruidRollup(
      SpecificDruidDimensions(Vector("hey", "what"), Vector.empty),
      Seq(new CountAggregatorFactory("heyyo")),
      QueryGranularity.NONE
    )
    rollup.validate()
  }

  test("Validations: Dimension and metric with the same name") {
    val e = the[IllegalArgumentException] thrownBy {
      DruidRollup(
        SpecificDruidDimensions(Vector("hey", "what"), Vector.empty),
        Seq(new CountAggregatorFactory("hey")),
        QueryGranularity.NONE
      )
    }
    e.getMessage must be("Duplicate columns: hey")
  }

  test("Validations: Two dimensions with the same name") {
    val e = the[ParseException] thrownBy {
      DruidRollup(
        SpecificDruidDimensions(Vector("what", "what"), Vector.empty),
        Seq(new CountAggregatorFactory("hey")),
        QueryGranularity.NONE
      )
    }
    e.getMessage must be("Duplicate column entries found : [what]")
  }

  test("Validations: Two metrics with the same name") {
    val e = the[IllegalArgumentException] thrownBy {
      DruidRollup(
        SpecificDruidDimensions(Vector("what"), Vector.empty),
        Seq(new CountAggregatorFactory("hey"), new LongSumAggregatorFactory("hey", "blah")),
        QueryGranularity.NONE
      )
    }
    e.getMessage must be("Duplicate columns: hey")
  }
}
