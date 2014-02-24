/*
 * Tranquility.
 * Copyright (C) 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.tranquility.beam

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event._
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.exception._
import com.metamx.common.scala.option._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.{Future, FuturePool}
import io.druid.segment.IndexGranularity
import java.util.UUID
import java.util.concurrent.Executors
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.joda.time.{Period, Interval, DateTime}
import org.scala_tools.time.Implicits._

/**
 * Beam composed of a stack of smaller beams. The smaller beams are split across two axes: timestamp (time shard
 * of the data) and partition (shard of the data within one time interval). The stack of beams for a particular
 * timestamp are created in a coordinated fashion, such that all ClusteredBeams for the same identifier will have
 * semantically identical stacks. This interaction is mediated through zookeeper. Beam information persists across
 * ClusteredBeam restarts.
 *
 * In the case of Druid, each merged beam corresponds to one segment partition number, and each inner beam corresponds
 * to either one index task or a set of redundant index tasks.
 *
 * {{{
 *                                            ClusteredBeam
 *
 *                                   +-------------+---------------+
 *               2010-01-02T03:00:00 |                             |   2010-01-02T04:00:00
 *                                   |                             |
 *                                   v                             v
 *
 *                         +----+ Merged +----+                   ...
 *                         |                  |
 *                    partition 1         partition 2
 *                         |                  |
 *                         v                  v
 *
 *                     Decorated           Decorated
 *
 *                   InnerBeamType       InnerBeamType
 * }}}
 */
class ClusteredBeam[EventType: Timestamper, InnerBeamType <: Beam[EventType]](
  zkBasePath: String,
  identifier: String,
  tuning: ClusteredBeamTuning,
  curator: CuratorFramework,
  emitter: ServiceEmitter,
  timekeeper: Timekeeper,
  objectMapper: ObjectMapper,
  beamMaker: BeamMaker[EventType, InnerBeamType],
  beamDecorateFn: (Interval, Int) => Beam[EventType] => Beam[EventType],
  beamMergeFn: Seq[Beam[EventType]] => Beam[EventType],
  alertMap: Dict
) extends Beam[EventType] with Logging
{
  require(tuning.partitions > 0, "tuning.partitions > 0")

  // Thread pool for blocking zk operations
  private[this] val zkFuturePool = FuturePool(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("ClusteredBeam-ZkFuturePool-%s" format UUID.randomUUID)
        .build()
    )
  )

  // Location of beam-related metadata in ZooKeeper.
  private[this] def zpath(path: String): String = {
    require(path.nonEmpty, "path must be nonempty")
    "%s/%s/%s" format(zkBasePath, identifier, path)
  }

  private[this] def zpathWithDefault(path: String, default: => Array[Byte]): String = {
    zpath(path) withEffect {
      p =>
        if (curator.checkExists().forPath(p) == null) {
          try {
            curator.create().creatingParentsIfNeeded().forPath(p, default)
          }
          catch {
            case e: NodeExistsException => // suppress
          }
        }
    }
  }

  // Mutex for modifying beam metadata.
  private[this] val mutex = new InterProcessSemaphoreMutex(curator, zpath("mutex"))

  // We will refuse to create beams earlier than this timestamp. The purpose of this is to prevent recreating beams
  // that we thought were closed.
  @volatile private[this] var latestTime = new DateTime(0)

  // Beams we are currently aware of.
  private[this] val beams = ConcurrentMap[DateTime, Beam[EventType]]()

  // Lock updates to "latestTime" and "beams" to prevent races.
  private[this] val beamWriteMonitor = new AnyRef

  private[this] lazy val data = new {
    val dataPath = zpathWithDefault("data", ClusteredBeamMeta.empty.toBytes(objectMapper))

    def modify(f: ClusteredBeamMeta => ClusteredBeamMeta): Future[ClusteredBeamMeta] = zkFuturePool {
      mutex.acquire()
      try {
        val prevMeta = ClusteredBeamMeta.fromBytes(objectMapper, curator.getData.forPath(dataPath)).fold(
          e => {
            emitAlert(e, log, emitter, WARN, "Failed to read beam data from cache: %s" format identifier, alertMap)
            throw e
          },
          meta => meta
        )
        val newMeta = f(prevMeta)
        if (newMeta != prevMeta) {
          log.info("Writing new beam data to: %s", dataPath)
          curator.setData().forPath(dataPath, newMeta.toBytes(objectMapper))
        }
        newMeta
      }
      finally {
        mutex.release()
      }
    }
  }

  @volatile private[this] var open = true

  private[this] def beam(timestamp: DateTime, now: DateTime): Future[Beam[EventType]] = {
    val bucket = tuning.segmentBucket(timestamp)
    val creationInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start,
      tuning.segmentBucket(Seq(now + tuning.warmingPeriod, now + tuning.windowPeriod).maxBy(_.millis)).end
    )
    val windowInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start,
      tuning.segmentBucket(now + tuning.windowPeriod).end
    )
    val futureBeamOption = beams.get(timestamp) match {
      case _ if !open => Future.value(None)
      case Some(x) if windowInterval.overlaps(bucket) => Future.value(Some(x))
      case Some(x) => Future.value(None)
      case None if timestamp <= latestTime => Future.value(None)
      case None if !creationInterval.overlaps(bucket) => Future.value(None)
      case None =>
        // We may want to create a new beam. Acquire the zk mutex and examine the situation.
        // This could be more efficient, but it's happening infrequently so it's probably not a big deal.
        @volatile var newBeamsOption: Option[Seq[InnerBeamType]] = None
        def closeNewBeams(): Future[Unit] = {
          // Wait for closing, but ignore exceptions.
          newBeamsOption match {
            case Some(newBeams) => Future.collect(newBeams map (_.close())).map(_ => ())
            case None => Future.Done
          }
        }
        data.modify {
          prev =>
            if (prev.beamDictss.contains(timestamp)) {
              log.info("Beams already created for identifier[%s] timestamp[%s]", identifier, timestamp)
              prev
            } else {
              log.info("Creating beams for identifier[%s] timestamp[%s]", identifier, timestamp)
              newBeamsOption = Some(
                (0 until tuning.partitions) map {
                  partition =>
                    val beam = beamMaker.newBeam(bucket, partition)
                    log.info("Created beam: %s", objectMapper.writeValueAsString(beamMaker.toDict(beam)))
                    beam
                }
              )
              val newLatestTime = Seq(prev.latestTime, timestamp).maxBy(_.millis)
              val newBeamDicts = (prev.beamDictss filterNot {
                case (ts, beam) =>
                  // Expire old beamDicts
                  tuning.segmentGranularity.increment(ts) + tuning.windowPeriod < now
              }) ++ Map(timestamp -> newBeamsOption.get.map(beamMaker.toDict))
              ClusteredBeamMeta(newLatestTime, newBeamDicts)
            }
        } rescue {
          case e: Throwable =>
            closeNewBeams() flatMap (_ => Future.exception(
              new IllegalStateException(
                "Failed to save new beam for identifier[%s] timestamp[%s]" format(identifier, timestamp), e
              )
            ))
        } map {
          meta =>
          // Update local stuff with our goodies from zk.
            beamWriteMonitor.synchronized {
              latestTime = meta.latestTime
              val mergedBeamOption = newBeamsOption map {
                newBeams =>
                  val decoratedBeams = for {
                    (newBeam, index) <- newBeams.zipWithIndex
                  } yield {
                    beamDecorateFn(bucket, index)(newBeam)
                  }
                  beamMergeFn(decoratedBeams)
              }
              mergedBeamOption foreach {
                mergedBeam =>
                // Use the new beam we created.
                  if (!beams.contains(timestamp)) {
                    beams(timestamp) = mergedBeam
                  } else {
                    // Close asynchronously, ignore return value
                    log.warn("WTF?! Already had beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                    mergedBeam.close()
                  }
              }
              for ((timestamp, beamDicts) <- meta.beamDictss -- beams.keys) {
                log.info("Adding beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                // Should have better handling of unparseable zk data. Changing BeamMaker implementations currently
                // just causes exceptions until the old dicts are cleared out.
                beams(timestamp) = beamMergeFn(
                  beamDicts map {
                    beamDict =>
                      val decorate = beamDecorateFn(
                        beamMaker.intervalFromDict(beamDict),
                        beamMaker.partitionFromDict(beamDict)
                      )
                      decorate(beamMaker.fromDict(beamDict))
                  }
                )
              }
              for ((timestamp, beam) <- beams -- meta.beamDictss.keys) {
                log.info("Removing beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                // Close asynchronously, ignore return value
                beams(timestamp).close()
                beams.remove(timestamp)
              }
              // Return requested beam
              beams.get(timestamp) ifEmpty {
                log.warn(
                  "WTF?! We should have acquired a beam for identifier[%s] timestamp[%s], but didn't!",
                  identifier,
                  timestamp
                )
              }
            }
        }
    }
    futureBeamOption map {
      beamOpt =>
      // If we didn't find a beam, then create a special dummy beam just for this batch. This allows us to apply
      // any merge or decorator logic to dropped events, which is nice if there are side effects (such as metrics
      // emission, logging, or alerting).
        beamOpt.getOrElse(
          beamMergeFn(
            (0 until tuning.partitions) map {
              partition =>
                beamDecorateFn(bucket, partition)(new NoopBeam[EventType])
            }
          )
        )
    }
  }

  def propagate(events: Seq[EventType]) = {
    val timestamper = implicitly[Timestamper[EventType]].timestamp _
    val grouped = events.groupBy(x => tuning.segmentBucket(timestamper(x)).start).toSeq.sortBy(_._1.millis)
    // Possibly warm up future beams
    def toBeWarmed(dt: DateTime, end: DateTime): List[DateTime] = {
      if (dt <= end) {
        dt :: toBeWarmed(tuning.segmentBucket(dt).end, end)
      } else {
        Nil
      }
    }
    for (
      latestEvent <- grouped.lastOption.map(_._2.maxBy(timestamper(_).millis)).map(timestamper);
      tbwTimestamp <- toBeWarmed(latestEvent, latestEvent + tuning.warmingPeriod) if tbwTimestamp > latestEvent
    ) {
      // Create beam asynchronously
      beam(tbwTimestamp, timekeeper.now)
    }
    // Propagate data
    val countFutures = for ((timestamp, eventGroup) <- grouped) yield {
      beam(timestamp, timekeeper.now) onFailure {
        e =>
          emitAlert(e, log, emitter, WARN, "Failed to create beams: %s" format identifier, alertMap)
      } flatMap {
        beam =>
        // We expect beams to handle retries, so if we get an exception here let's drop the batch
          beam.propagate(eventGroup) rescue {
            case e: DefunctBeamException =>
              // Just drop data until the next segment starts. At some point we should look at doing something
              // more intelligent.
              emitAlert(
                e, log, emitter, WARN, "Beams defunct: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
                    "beam" -> beam.toString
                  )
              )
              data.modify {
                prev =>
                  prev.copy(beamDictss = prev.beamDictss - timestamp)
              } onSuccess {
                meta =>
                  beamWriteMonitor.synchronized {
                    beams.remove(timestamp)
                  }
              } map (_ => 0)

            case e: Exception =>
              emitAlert(
                e, log, emitter, WARN, "Failed to propagate events: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
                    "beams" -> beam.toString
                  )
              )
              Future.value(0)
          }
      }
    }
    Future.collect(countFutures).map(_.sum)
  }

  def close() = {
    beamWriteMonitor.synchronized {
      open = false
      val closeFuture = Future.collect(beams.values.toList map (_.close())) map (_ => ())
      beams.clear()
      closeFuture
    }
  }

  override def toString = "ClusteredBeam(%s)" format identifier
}

case class ClusteredBeamMeta(latestTime: DateTime, beamDictss: Map[DateTime, Seq[Dict]])
{
  def toBytes(objectMapper: ObjectMapper) = objectMapper.writeValueAsBytes(
    Dict(
      "latestTime" -> latestTime.toString(),
      "beams" -> beamDictss
    )
  )
}

object ClusteredBeamMeta
{
  def empty = ClusteredBeamMeta(new DateTime(0), Map.empty)

  def fromBytes[A](objectMapper: ObjectMapper, bytes: Array[Byte]): Either[Exception, ClusteredBeamMeta] = {
    try {
      val d = objectMapper.readValue(bytes, classOf[Dict])
      val beams: Map[DateTime, Seq[Dict]] = dict(d.getOrElse("beams", Dict())) map {
        case (k, vs) =>
          val ts = new DateTime(k)
          val beamDicts = list(vs) map (dict(_))
          (ts, beamDicts)
      }
      val latestTime = new DateTime(d.getOrElse("latestTime", 0L)) swallow {
        case e: Exception =>
      }
      Right(ClusteredBeamMeta((beams.keys ++ latestTime).maxBy(_.millis), beams))
    }
    catch {
      case e: Exception =>
        Left(e)
    }
  }
}

case class ClusteredBeamTuning(
  segmentGranularity: Granularity,
  warmingPeriod: Period,
  windowPeriod: Period,
  partitions: Int,
  replicants: Int
)
{
  def segmentGranularityAsIndexGranularity: IndexGranularity = segmentGranularity match {
    case Granularity.MINUTE => IndexGranularity.MINUTE
    case Granularity.HOUR => IndexGranularity.HOUR
    case Granularity.DAY => IndexGranularity.DAY
    case x =>
      throw new IllegalArgumentException("No IndexGranularity for granularity: %s" format x)
  }

  def segmentBucket(ts: DateTime) = segmentGranularity.bucket(ts)
}

object ClusteredBeamTuning
{
  /**
   * Factory method for ClusteredBeamTuning objects.
   *
   * @param segmentGranularity Each sub-beam will cover blocks of this size in the timeline. This controls how often
   *                           segments are closed off and made immutable. {{{Granularity.HOUR}}} is usually reasonable.
   * @param warmingPeriod If nonzero, create sub-beams this early. This can be useful if sub-beams take a long time
   *                      to start up.
   * @param windowPeriod Accept events this far outside of their timeline block. e.g. with a windowPeriod of 10 minutes,
   *                     and segmentGranularity of HOUR, we will accept an event timestamped for 4:15PM anywhere from
   *                     3:50PM to 4:10PM.
   * @param partitions Create this many logically distinct sub-beams per timeline block. This is used to scale
   *                   ingestion up to handle larger streams.
   * @param replicants Create this many replicants per sub-beam. This is used to provide higher availability and
   *                   parallelism for queries.
   */
  def create(
    segmentGranularity: Granularity,
    warmingPeriod: Period,
    windowPeriod: Period,
    partitions: Int,
    replicants: Int
  ): ClusteredBeamTuning = {
    apply(segmentGranularity, warmingPeriod, windowPeriod, partitions, replicants)
  }
}
