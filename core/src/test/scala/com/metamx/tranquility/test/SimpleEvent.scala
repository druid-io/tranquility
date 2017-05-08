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

import com.fasterxml.jackson.annotation.JsonValue
import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.test.DirectDruidTest.TimeColumn
import com.metamx.tranquility.typeclass.Timestamper

case class SimpleEvent(ts: DateTime, foo: String, bar: Int, lat: Double, lon: Double)
{
  @JsonValue
  def toMap: Map[String, Any] = Map(
    TimeColumn -> (ts.getMillis / 1000),
    "foo" -> foo,
    "bar" -> bar,
    "lat" -> lat,
    "lon" -> lon
  )

  def toNestedMap: Map[String, Any] = Map(
    TimeColumn -> (ts.getMillis / 1000),
    "data" -> Map("foo" -> foo, "bar" -> bar),
    "geo" -> Map("lat" -> lat, "lon" -> lon)
  )

  def toCsv: String = Seq(ts.getMillis / 1000, foo, bar, lat, lon).mkString(",")
}

object SimpleEvent
{
  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }

  val Columns = Seq(TimeColumn, "foo", "bar", "lat", "lon")

  def fromMap(d: Dict): SimpleEvent = {
    SimpleEvent(
      new DateTime(long(d(TimeColumn)) * 1000),
      str(d("foo")),
      int(d("bar")),
      double(d("lat")),
      double(d("lon"))
    )
  }
}
