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

package com.metamx.tranquility.server

import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.server.http.DataSourceBundle
import com.metamx.tranquility.server.http.TranquilityServlet
import com.metamx.tranquility.tranquilizer.Tranquilizer
import io.druid.data.input.InputRow
import io.druid.data.input.impl.DimensionsSpec
import io.druid.data.input.impl.ParseSpec
import io.druid.data.input.impl.TimeAndDimsParseSpec
import io.druid.data.input.impl.TimestampSpec
import org.joda.time.DateTime
import org.scalatra.test.ScalatraTests

object ServerTestUtil
{
  def withTester(
    beams: Map[String, Beam[InputRow]],
    parseSpecs: Map[String, ParseSpec] = Map.empty
  )(f: ScalatraTests => Unit): Unit =
  {
    val tester = new ScalatraTests {}
    val bundles = (beams.keys map { dataSource =>
      val beam = beams(dataSource)
      val tranquilizer: Tranquilizer[InputRow] = Tranquilizer.create(beam)
      tranquilizer.start()
      val parseSpec = parseSpecs.getOrElse(
        dataSource,
        new TimeAndDimsParseSpec(
          new TimestampSpec("ts", "posix", new DateTime(0)),
          new DimensionsSpec(null, null, null)
        )
      )
      (dataSource, new DataSourceBundle(tranquilizer, parseSpec))
    }).toMap
    val servlet = new TranquilityServlet(bundles)
    tester.addServlet(servlet, "/*")
    tester.start()
    try {
      f(tester)
    }
    finally {
      tester.stop()
      bundles.values.foreach(_.tranquilizer.flush())
      bundles.values.foreach(_.tranquilizer.stop())
    }
  }
}
