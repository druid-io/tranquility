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
package com.metamx.tranquility.finagle

import com.metamx.tranquility.beam.Beam
import com.twitter.finagle.Service
import com.twitter.util.Time

/**
 * Bridges Beams with Finagle Services.
 */
class BeamService[A](beam: Beam[A]) extends Service[Seq[A], Int]
{
  def apply(request: Seq[A]) = beam.propagate(request)

  override def close(deadline: Time) = beam.close()
}
