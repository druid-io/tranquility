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
package com.metamx.tranquility.beam

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.untyped._

/**
 * Makes beams for particular intervals and partition numbers. Can also serialize and deserialize beam representations.
 *
 * @tparam A event type
 * @tparam BeamType specific beam type we know how to create
 */
trait BeamMaker[A, BeamType <: Beam[A]]
{
  def newBeam(interval: Interval, partition: Int): BeamType

  def toDict(beam: BeamType): Dict

  def fromDict(d: Dict): BeamType
}
