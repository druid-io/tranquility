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

import com.metamx.common.Granularity
import com.metamx.tranquility.druid.DruidBeamMaker
import org.joda.time.DateTime
import org.scalatest.FunSuite

class DruidBeamTest extends FunSuite
{

  test("GenerateFirehoseId")
  {
    val dt = new DateTime("2010-02-03T12:34:56.789Z")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.MINUTE, dt, 1) === "x-34-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.HOUR, dt, 1) === "x-12-0001")
    assert(DruidBeamMaker.generateBaseFirehoseId("x", Granularity.DAY, dt, 1) === "x-03-0001")
  }

}
