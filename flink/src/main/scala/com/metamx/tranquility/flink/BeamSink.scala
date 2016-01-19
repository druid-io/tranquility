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
package com.metamx.tranquility.flink

import com.metamx.common.scala.Logging
import com.metamx.tranquility.tranquilizer.SimpleTranquilizerAdapter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * This class provides a sink that can propagate any event type to Druid.
  * @param beamFactory your implementation of [[BeamFactory]].
  */
class BeamSink[T](beamFactory: BeamFactory[T])
  extends RichSinkFunction[T] with Logging
{
  var sender: Option[SimpleTranquilizerAdapter[T]] = None

  override def open(parameters: Configuration) = {
    sender = Some(beamFactory.tranquilizer.simple(false))
  }

  override def invoke(value: T) = sender.get.send(value)

  override def close() = sender.get.flush()
}
