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
package com.metamx.tranquility.storm

import backtype.storm.task.IMetricsContext
import com.metamx.common.scala.Logging
import com.metamx.tranquility.beam.Beam
import com.twitter.util.Await
import scala.collection.JavaConverters._
import storm.trident.operation.TridentCollector
import storm.trident.state.{StateFactory, BaseStateUpdater, State}
import storm.trident.tuple.TridentTuple

/**
 * A Trident State for using Beams to propagate tuples.
 */
class TridentBeamState[EventType](beam: Beam[EventType])
  extends State with Logging
{
  // We could use this to provide exactly-once semantics one day.
  var txid: Option[Long] = None

  def send(events: Seq[EventType]): Int = {
    log.debug("Sending %,d events with txid[%s]", events.size, txid.getOrElse("none"))
    Await.result(beam.sendBatch(events)).size
  }

  def close() {
    Await.result(beam.close())
  }

  override def beginCommit(txid: java.lang.Long) {
    this.txid = Some(txid)
  }

  override def commit(txid: java.lang.Long) {
    this.txid = None
  }
}

class TridentBeamStateFactory[EventType](beamFactory: BeamFactory[EventType])
  extends StateFactory with Logging
{
  override def makeState(
    conf: java.util.Map[_, _],
    metrics: IMetricsContext,
    partitionIndex: Int,
    numPartitions: Int
  ) = {
    new TridentBeamState(beamFactory.makeBeam(conf, metrics))
  }
}

/**
 * A Trident StateUpdater for use with BeamTridentStates.
 */
class TridentBeamStateUpdater[EventType] extends BaseStateUpdater[TridentBeamState[EventType]]
{
  @transient
  @volatile private[this] var stateToCleanup: TridentBeamState[EventType] = null

  override def updateState(
    state: TridentBeamState[EventType],
    tuples: java.util.List[TridentTuple],
    collector: TridentCollector
  )
  {
    // Not sure if these checks are necessary; can a StateUpdater be called from more than one thread?
    if (stateToCleanup == null) {
      synchronized {
        if (stateToCleanup == null) {
          stateToCleanup = state
        }
      }
    }

    if (stateToCleanup ne state) {
      throw new IllegalStateException("WTF?! Got more than one state!")
    }

    state.send(tuples.asScala map (tuple => tuple.getValue(0).asInstanceOf[EventType]))
  }

  override def cleanup() {
    Option(stateToCleanup) foreach (_.close())
  }
}
