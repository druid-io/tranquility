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

import com.metamx.common.scala.collection.implicits._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.TransformingBeam
import com.metamx.tranquility.server.http.TranquilityServlet
import com.metamx.tranquility.tranquilizer.Tranquilizer
import org.scalatra.test.ScalatraTests
import scala.collection.JavaConverters._

object ServerTestUtil
{
  def withTester(beams: Map[String, Beam[Dict]])(f: ScalatraTests => Unit): Unit = {
    val tester = new ScalatraTests {}
    val tranquilizers = beams strictMapValues { beam =>
      val t = Tranquilizer.create(
        new TransformingBeam[java.util.Map[String, AnyRef], Dict](
          beam,
          _.asScala.toMap
        )
      )
      t.start()
      t
    }
    val servlet = new TranquilityServlet(tranquilizers)
    tester.addServlet(servlet, "/*")
    tester.start()
    try {
      f(tester)
    }
    finally {
      tester.stop()
      tranquilizers.values.foreach(_.flush())
      tranquilizers.values.foreach(_.stop())
    }
  }
}
