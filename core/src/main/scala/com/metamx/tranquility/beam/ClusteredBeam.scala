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

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.nscala_time.time.Imports._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event._
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.option._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util._
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.joda.time.chrono.ISOChronology
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.Random

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
  require(tuning.minSegmentsPerBeam > 0, "tuning.minSegmentsPerBeam > 0")
  require(
    tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam,
    "tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam"
  )

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
  @volatile private[this] var localLatestCloseTime = new DateTime(0, ISOChronology.getInstanceUTC)

  private[this] val rand = new Random

  // Merged beams we are currently aware of, interval start millis -> merged beam.
  private[this] val beams = ConcurrentMap[Long, Beam[EventType]]()

  // Lock updates to "localLatestCloseTime" and "beams" to prevent races.
  private[this] val beamWriteMonitor = new AnyRef

  private[this] lazy val data = new {
    val dataPath = zpathWithDefault("data", ClusteredBeamMeta.empty.toBytes(objectMapper))

    def modify(f: ClusteredBeamMeta => ClusteredBeamMeta): Future[ClusteredBeamMeta] = zkFuturePool {
      mutex.acquire()
      try {
        curator.sync().forPath(dataPath)
        val prevMeta = ClusteredBeamMeta.fromBytes(objectMapper, curator.getData.forPath(dataPath)).fold(
          e => {
            emitAlert(e, log, emitter, WARN, "Failed to read beam data from cache: %s" format identifier, alertMap)
            throw e
          },
          meta => meta
        )
        val newMeta = f(prevMeta)
        if (newMeta != prevMeta) {
          val newMetaBytes = newMeta.toBytes(objectMapper)
          log.info("Writing new beam data to[%s]: %s", dataPath, new String(newMetaBytes))
          curator.setData().forPath(dataPath, newMetaBytes)
        }
        newMeta
      }
      catch {
        case e: Throwable =>
          // Log Throwables to avoid invisible errors caused by https://github.com/twitter/util/issues/100.
          log.error(e, "Failed to update cluster state: %s", identifier)
          throw e
      }
      finally {
        mutex.release()
      }
    }
  }

  @volatile private[this] var open = true

  val timestamper: EventType => DateTime = {
    val theImplicit = implicitly[Timestamper[EventType]].timestamp _
    t => theImplicit(t).withZone(DateTimeZone.UTC)
  }

  private[this] def beam(timestamp: DateTime, now: DateTime): Future[Beam[EventType]] = {
    val bucket = tuning.segmentBucket(timestamp)
    val creationInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start.getMillis,
      tuning.segmentBucket(Seq(now + tuning.warmingPeriod, now + tuning.windowPeriod).maxBy(_.getMillis)).end.getMillis,
      ISOChronology.getInstanceUTC
    )
    val windowInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start.getMillis,
      tuning.segmentBucket(now + tuning.windowPeriod).end.getMillis,
      ISOChronology.getInstanceUTC
    )
    val futureBeamOption = beams.get(timestamp.getMillis) match {
      case _ if !open => Future.value(None)
      case Some(x) if windowInterval.overlaps(bucket) => Future.value(Some(x))
      case Some(x) => Future.value(None)
      case None if timestamp <= localLatestCloseTime => Future.value(None)
      case None if !creationInterval.overlaps(bucket) => Future.value(None)
      case None =>
        // We may want to create new merged beam(s). Acquire the zk mutex and examine the situation.
        // This could be more efficient, but it's happening infrequently so it's probably not a big deal.
        data.modify {
          prev =>
            val prevBeamDicts = prev.beamDictss.getOrElse(timestamp.getMillis, Nil)
            if (prevBeamDicts.size >= tuning.partitions) {
              log.info(
                "Merged beam already created for identifier[%s] timestamp[%s], with sufficient partitions (target = %d, actual = %d)",
                identifier,
                timestamp,
                tuning.partitions,
                prevBeamDicts.size
              )
              prev
            } else if (timestamp <= prev.latestCloseTime) {
              log.info(
                "Global latestCloseTime[%s] for identifier[%s] has moved past timestamp[%s], not creating merged beam",
                prev.latestCloseTime,
                identifier,
                timestamp
              )
              prev
            } else {
              assert(prevBeamDicts.size < tuning.partitions)
              assert(timestamp > prev.latestCloseTime)

              // We might want to cover multiple time segments in advance.
              val numSegmentsToCover = tuning.minSegmentsPerBeam +
                rand.nextInt(tuning.maxSegmentsPerBeam - tuning.minSegmentsPerBeam + 1)
              val intervalToCover = new Interval(
                timestamp.getMillis,
                tuning.segmentGranularity.increment(timestamp, numSegmentsToCover).getMillis,
                ISOChronology.getInstanceUTC
              )
              val timestampsToCover = tuning.segmentGranularity.getIterable(intervalToCover).asScala.map(_.start)

              // OK, create them where needed.
              val newInnerBeamDictsByPartition = new mutable.HashMap[Int, Dict]
              val newBeamDictss: Map[Long, Seq[Dict]] = (prev.beamDictss filterNot {
                case (millis, beam) =>
                  // Expire old beamDicts
                  tuning.segmentGranularity.increment(new DateTime(millis)) + tuning.windowPeriod < now
              }) ++ (for (ts <- timestampsToCover) yield {
                val tsPrevDicts = prev.beamDictss.getOrElse(ts.getMillis, Nil)
                log.info(
                  "Creating new merged beam for identifier[%s] timestamp[%s] (target = %d, actual = %d)",
                  identifier,
                  ts,
                  tuning.partitions,
                  tsPrevDicts.size
                )
                val tsNewDicts = tsPrevDicts ++ ((tsPrevDicts.size until tuning.partitions) map {
                  partition =>
                    newInnerBeamDictsByPartition.getOrElseUpdate(
                      partition, {
                        // Create sub-beams and then immediately close them, just so we can get the dict representations.
                        // Close asynchronously, ignore return value.
                        beamMaker.newBeam(intervalToCover, partition).withFinally(_.close()) {
                          beam =>
                            val beamDict = beamMaker.toDict(beam)
                            log.info("Created beam: %s", objectMapper.writeValueAsString(beamDict))
                            beamDict
                        }
                      }
                    )
                })
                (ts.getMillis, tsNewDicts)
              })
              val newLatestCloseTime = new DateTime(
                (Seq(prev.latestCloseTime.getMillis) ++ (prev.beamDictss.keySet -- newBeamDictss.keySet)).max,
                ISOChronology.getInstanceUTC
              )
              ClusteredBeamMeta(
                newLatestCloseTime,
                newBeamDictss
              )
            }
        } rescue {
          case e: Throwable =>
            Future.exception(
              new IllegalStateException(
                "Failed to save new beam for identifier[%s] timestamp[%s]" format(identifier, timestamp), e
              )
            )
        } map {
          meta =>
            // Update local stuff with our goodies from zk.
            beamWriteMonitor.synchronized {
              localLatestCloseTime = meta.latestCloseTime
              // Only add the beams we actually wanted at this time. This is because there might be other beams in ZK
              // that we don't want to add just yet, on account of maybe they need their partitions expanded (this only
              // happens when they are the necessary ones).
              if (!beams.contains(timestamp.getMillis) && meta.beamDictss.contains(timestamp.getMillis)) {
                val beamDicts = meta.beamDictss(timestamp.getMillis)
                log.info("Adding beams for identifier[%s] timestamp[%s]: %s", identifier, timestamp, beamDicts)
                // Should have better handling of unparseable zk data. Changing BeamMaker implementations currently
                // just causes exceptions until the old dicts are cleared out.
                beams(timestamp.getMillis) = beamMergeFn(
                  beamDicts.zipWithIndex map {
                    case (beamDict, partitionNum) =>
                      val decorate = beamDecorateFn(tuning.segmentBucket(timestamp), partitionNum)
                      decorate(beamMaker.fromDict(beamDict))
                  }
                )
              }
              // Remove beams that are gone from ZK metadata. They have expired.
              for ((timestamp, beam) <- beams -- meta.beamDictss.keys) {
                log.info("Removing beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                // Close asynchronously, ignore return value.
                beams(timestamp).close()
                beams.remove(timestamp)
              }
              // Return requested beam. It may not have actually been created, so it's an Option.
              beams.get(timestamp.getMillis) ifEmpty {
                log.info(
                  "Turns out we decided not to actually make beams for identifier[%s] timestamp[%s]. Returning None.",
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

  override def sendAll(events: Seq[EventType]): Seq[Future[SendResult]] = {
    val now = timekeeper.now.withZone(DateTimeZone.UTC)
    // Events, grouped and ordered by truncated timestamp, with their original indexes remembered
    val eventsWithPromises = Vector() ++ events.map(event => (event, Promise[SendResult]()))
    val grouped: Seq[(DateTime, IndexedSeq[(EventType, Promise[SendResult])])] = (eventsWithPromises groupBy {
      case (event, promise) =>
        tuning.segmentBucket(timestamper(event)).start
    }).toSeq.sortBy(_._1.getMillis)
    // Possibly warm up future beams
    def toBeWarmed(dt: DateTime, end: DateTime): List[DateTime] = {
      if (dt <= end) {
        dt :: toBeWarmed(tuning.segmentBucket(dt).end, end)
      } else {
        Nil
      }
    }
    val latestEventTimestamp: Option[DateTime] = grouped.lastOption map { case (truncatedTimestamp, group) =>
      val event: EventType = group.maxBy(tuple => timestamper(tuple._1).getMillis)._1
      timestamper(event)
    }
    val warmingBeams: Future[Seq[Beam[EventType]]] = Future.collect(
      for (
        latest <- latestEventTimestamp.toList;
        tbwTimestamp <- toBeWarmed(latest, latest + tuning.warmingPeriod) if tbwTimestamp > latest
      ) yield {
        // Create beam asynchronously
        beam(tbwTimestamp, now)
      }
    )
    // Send data
    for ((timestamp, eventGroup) <- grouped) {
      val futureOfFutures: Future[Seq[Future[SendResult]]] = beam(timestamp, now) transform {
        case Throw(e) =>
          // Could not generate beam, fail everything.
          emitAlert(e, log, emitter, WARN, "Failed to create merged beam: %s" format identifier, alertMap)
          val throwMe = new IllegalStateException(s"Failed to create merged beam: $identifier", e)
          Future.value(eventGroup.map(_ => Future.exception(throwMe)))

        case Return(theBeam) =>
          // We expect beams to handle retries, so if we get an exception here let's convert them to drops.
          val rawFutures: Seq[Future[SendResult]] = theBeam.sendAll(eventGroup.map(_._1))
          val sawDefunct = new AtomicBoolean
          val sawOtherException = new AtomicBoolean

          val rescuedFutures = for (rawFuture <- rawFutures) yield {
            // Error handling
            rawFuture rescue {
              case e: DefunctBeamException =>
                if (sawDefunct.compareAndSet(false, true)) {
                  emitAlert(
                    e, log, emitter, WARN, "Beam defunct: %s" format identifier,
                    alertMap ++
                      Dict(
                        "eventCount" -> eventGroup.size,
                        "timestamp" -> timestamp.toString(),
                        "beam" -> theBeam.toString
                      )
                  )
                  data.modify {
                    prev =>
                      ClusteredBeamMeta(
                        Seq(prev.latestCloseTime, timestamp).maxBy(_.getMillis),
                        prev.beamDictss - timestamp.getMillis
                      )
                  } onSuccess {
                    meta =>
                      beamWriteMonitor.synchronized {
                        beams.remove(timestamp.getMillis)
                      }
                  } map (_ => SendResult.Dropped)
                } else {
                  Future(SendResult.Dropped)
                }

              case e: Exception =>
                if (sawOtherException.compareAndSet(false, true)) {
                  emitAlert(
                    e, log, emitter, WARN, "Failed to propagate events: %s" format identifier,
                    alertMap ++
                      Dict(
                        "eventCount" -> eventGroup.size,
                        "timestamp" -> timestamp.toString(),
                        "beams" -> theBeam.toString
                      )
                  )
                }
                Future(SendResult.Dropped)
            }
          }

          Future.value(rescuedFutures)
      }

      futureOfFutures onSuccess { futures =>
        for (((event, promise), future) <- eventGroup zip futures) {
          promise.become(future)
        }
      } onFailure { e =>
        log.error(e, "WTF?! Did not expect futureOfFutures to fail...")
        for ((event, promise) <- eventGroup) {
          promise.setException(e)
        }
      }
    }
    eventsWithPromises map { case (event, promise) =>
      // Resolve only when future beams are warmed up.
      warmingBeams.flatMap(_ => promise)
    }
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

/**
  * Metadata stored in ZooKeeper for a ClusteredBeam.
  *
  * @param latestCloseTime Most recently shut-down interval (to prevent necromancy).
  * @param beamDictss      Map of interval start -> beam metadata, partition by partition.
  */
case class ClusteredBeamMeta(latestCloseTime: DateTime, beamDictss: Map[Long, Seq[Dict]])
{
  def toBytes(objectMapper: ObjectMapper) = objectMapper.writeValueAsBytes(
    Dict(
      // latestTime is only being written for backwards compatibility
      "latestTime" -> new DateTime(
        (Seq(latestCloseTime.getMillis) ++ beamDictss.map(_._1)).max,
        ISOChronology.getInstanceUTC
      ).toString(),
      "latestCloseTime" -> latestCloseTime.toString(),
      "beams" -> beamDictss.map(kv => (new DateTime(kv._1, ISOChronology.getInstanceUTC).toString(), kv._2))
    )
  )
}

object ClusteredBeamMeta
{
  def empty = ClusteredBeamMeta(new DateTime(0, ISOChronology.getInstanceUTC), Map.empty)

  def fromBytes[A](objectMapper: ObjectMapper, bytes: Array[Byte]): Either[Exception, ClusteredBeamMeta] = {
    try {
      val d = objectMapper.readValue(bytes, classOf[Dict])
      val beams: Map[Long, Seq[Dict]] = dict(d.getOrElse("beams", Dict())) map {
        case (k, vs) =>
          val ts = new DateTime(k, ISOChronology.getInstanceUTC)
          val beamDicts = list(vs) map (dict(_))
          (ts.getMillis, beamDicts)
      }
      val latestCloseTime = new DateTime(d.getOrElse("latestCloseTime", 0L), ISOChronology.getInstanceUTC)
      Right(ClusteredBeamMeta(latestCloseTime, beams))
    }
    catch {
      case e: Exception =>
        Left(e)
    }
  }
}
