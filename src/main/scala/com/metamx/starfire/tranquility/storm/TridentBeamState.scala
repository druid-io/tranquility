package com.metamx.starfire.tranquility.storm

import backtype.storm.task.IMetricsContext
import com.metamx.common.scala.Logging
import com.metamx.starfire.tranquility.beam.Beam
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
    log.info("Sending %,d events with txid[%s]", events.size, txid.getOrElse("none"))
    Await.result(beam.propagate(events))
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
