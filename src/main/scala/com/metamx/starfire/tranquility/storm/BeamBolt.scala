package com.metamx.starfire.tranquility.storm

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent.loggingRunnable
import com.metamx.starfire.tranquility.beam.Beam
import com.twitter.util.Await
import java.util.concurrent.LinkedBlockingQueue
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * A Storm Bolt for using a Beam to propagate tuples.
 * @param beamFactory a factory for creating the beam we will use
 * @param confPrefixOption extract configuration properties using this prefix. For example, if the prefix is "foo",
 *                         we'll use foo.queue.size for the blocking queue size.
 */
class BeamBolt[EventType](beamFactory: BeamFactory[EventType], confPrefixOption: Option[String])
  extends BaseRichBolt with Logging
{
  def this(beamFactory: BeamFactory[EventType]) = this(beamFactory, None)

  def this(beamFactory: BeamFactory[EventType], confPrefix: String) = this(beamFactory, Some(confPrefix))

  @volatile private var beam      : Beam[EventType] = null
  @volatile private var queueSize : Int             = 1000
  @volatile private var emitMillis: Long            = 1000
  @volatile private var emitThread: Thread          = null

  private val queue = new LinkedBlockingQueue[Tuple]()

  override def prepare(conf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    val confPrefix = confPrefixOption map (_ + ".") getOrElse ""
    Option(conf.get("%squeue.size" format confPrefix)) foreach (v => queueSize = v.asInstanceOf[String].toInt)
    Option(conf.get("%semit.millis" format confPrefix)) foreach (v => emitMillis = v.asInstanceOf[String].toLong)
    beam = beamFactory.makeBeam(conf, context)
    val emitRunnable = loggingRunnable {
      while (!Thread.currentThread().isInterrupted) {
        val startMillis = System.currentTimeMillis()
        val emittableJava = new ju.ArrayList[Tuple]()
        val emittable = emittableJava.asScala
        queue.drainTo(emittableJava)
        if (emittable.nonEmpty) {
          try {
            val events: IndexedSeq[EventType] = emittable.map(_.getValue(0).asInstanceOf[EventType]).toIndexedSeq
            val sent = Await.result(beam.propagate(events))
            log.info("Sent %,d, ignored %,d queued events.", sent, events.size - sent)
            emittable foreach collector.ack
          }
          catch {
            case e: Exception =>
              log.warn("Failed to send %,d queued events.")
              emittable foreach collector.fail
          }
        }
        val waitMillis = startMillis + emitMillis - System.currentTimeMillis()
        if (waitMillis > 0) {
          Thread.sleep(waitMillis)
        }
      }
    }
    emitThread = new Thread(emitRunnable)
    emitThread.setName("BeamBolt-Emitter-%s" format confPrefix)
    emitThread.setDaemon(true)
    emitThread.start()
  }

  override def execute(tuple: Tuple) {
    queue.put(tuple)
  }

  override def cleanup() {
    Option(emitThread) foreach (_.interrupt())
    Await.ready(beam.close())
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
