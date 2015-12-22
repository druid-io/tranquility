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
package com.metamx.tranquility.druid

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.DefunctBeamException
import com.metamx.tranquility.finagle._
import com.metamx.tranquility.typeclass.ObjectWriter
import com.twitter.io.Buf
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import org.scala_tools.time.Imports._
import scala.collection.immutable.BitSet

/**
  * A Beam that writes all events to a fixed set of Druid tasks.
  */
class DruidBeam[A](
  private[druid] val interval: Interval,
  private[druid] val partition: Int,
  private[druid] val tasks: Seq[TaskPointer],
  location: DruidLocation,
  config: DruidBeamConfig,
  finagleRegistry: FinagleRegistry,
  indexService: IndexService,
  emitter: ServiceEmitter,
  objectWriter: ObjectWriter[A]
) extends Beam[A] with Logging with Closable
{
  private[this] val clients = Map(
    tasks map {
      task =>
        val service = location.environment.firehoseServicePattern format task.serviceKey
        task ->
          new TaskClient(
            task,
            finagleRegistry.checkout(service),
            location.dataSource,
            config.firehoseQuietPeriod,
            config.firehoseRetryPeriod,
            indexService,
            emitter
          )
    }: _*
  )

  override def sendBatch(events: Seq[A]): Future[BitSet] = {
    // Chunk payloads + indexed original events
    val eventsChunks: List[(Array[Byte], IndexedSeq[(A, Int)])] = Beam.index(events)
      .grouped(config.firehoseChunkSize)
      .map(xs => (objectWriter.batchAsBytes(xs.map(_._1)), xs))
      .toList
    // Futures will be the number of events pushed, or an exception. Zero events pushed means we gave up on the task.
    val taskChunkFutures: Seq[Future[(TaskPointer, BitSet)]] = for {
      (eventsChunkBytes, eventsChunk) <- eventsChunks
      task <- tasks
      client <- clients.get(task) if client.active
    } yield {
      val eventPost = HttpPost(
        "/druid/worker/v1/chat/%s/push-events" format
          (location.environment.firehoseServicePattern format task.serviceKey)
      ) withEffect {
        req =>
          req.headerMap("Content-Type") = objectWriter.contentType
          req.headerMap("Content-Length") = eventsChunkBytes.length.toString
          req.content = Buf.ByteArray.Shared(eventsChunkBytes)
      }
      if (log.isTraceEnabled) {
        log.trace(
          "Sending %,d events to task[%s], firehose[%s]: %s",
          eventsChunk,
          task.id,
          task.serviceKey,
          new String(eventsChunkBytes)
        )
      }
      client(eventPost) map {
        case Some(response) => task -> (BitSet.empty ++ eventsChunk.map(_._2))
        case None => task -> BitSet.empty
      }
    }
    val taskBitSetsFuture: Future[Map[TaskPointer, BitSet]] = Future.collect(taskChunkFutures) map {
      xs =>
        xs.groupBy(_._1).map {
          case (task, tuples: Seq[(TaskPointer, BitSet)]) =>
            task -> Beam.mergeBitsets(tuples.view.map(_._2))
        }
    }
    val finalFuture: Future[BitSet] = taskBitSetsFuture map {
      taskBitSets =>
        Beam.mergeBitsets(taskBitSets.values) withEffect { bitset =>
          if (bitset.isEmpty) {
            throw new DefunctBeamException("Tasks are all gone: %s" format tasks.map(_.id).mkString(", "))
          }
        }
    }
    finalFuture
  }

  override def close(deadline: Time): Future[Unit] = {
    log.info(
      "Closing Druid beam for datasource[%s] interval[%s] (tasks = %s)",
      location.dataSource,
      interval,
      tasks.map(_.id).mkString(", ")
    )
    Future.collect(clients.values.toList.map(client => client.close(deadline))) map (_ => ())
  }

  override def toString = "DruidBeam(interval = %s, partition = %s, tasks = [%s])" format
    (interval, partition, clients.values.map(t => "%s/%s" format(t.task.id, t.task.serviceKey)).mkString("; "))
}
