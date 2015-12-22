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

import com.twitter.util.Future

/**
 * Beams can accept events and forward them along. The propagate method may throw a DefunctBeamException, which means
 * the beam should be discarded (after calling close()).
 */
trait Beam[A]
{
  /**
   * Request propagation of events. The operation may fail in various ways, which tend to be specific to
   * the implementing class. Returns the number of events propagated.
   */
  def propagate(events: Seq[A]): Future[Int]

  /**
   * Signal that no more events will be sent. Many implementations need this to do proper cleanup.
   */
  def close(): Future[Unit]
}

class DefunctBeamException(s: String, t: Throwable) extends Exception(s, t)
{
  def this(s: String) = this(s, null)
}
