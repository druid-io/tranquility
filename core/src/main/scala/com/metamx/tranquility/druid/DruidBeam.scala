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

  override def propagate(events: Seq[A]) = {
    val eventsChunks = events
      .grouped(config.firehoseChunkSize)
      .map(xs => (objectWriter.batchAsBytes(xs), xs.size))
      .toList
    // Futures will be the number of events pushed, or an exception. Zero events pushed means we gave up on the task.
    val taskChunkFutures: Seq[Future[(TaskPointer, Int)]] = for {
      (eventsChunk, eventsChunkSize) <- eventsChunks
      task <- tasks
      client <- clients.get(task) if client.active
    } yield {
      val eventPost = HttpPost(
        "/druid/worker/v1/chat/%s/push-events" format
          (location.environment.firehoseServicePattern format task.serviceKey)
      ) withEffect {
        req =>
          req.headerMap("Content-Type") = objectWriter.contentType
          req.headerMap("Content-Length") = eventsChunk.length.toString
          req.content = Buf.ByteArray.Shared(eventsChunk)
      }
      if (log.isTraceEnabled) {
        log.trace(
          "Sending %,d events to task[%s], firehose[%s]: %s",
          eventsChunkSize,
          task.id,
          task.serviceKey,
          new String(eventsChunk)
        )
      }
      client(eventPost) map {
        case Some(response) => task -> eventsChunkSize
        case None => task -> 0
      }
    }
    val taskSuccessesFuture: Future[Map[TaskPointer, Int]] = Future.collect(taskChunkFutures) map {
      xs =>
        xs.groupBy(_._1).map {
          case (task, tuples) =>
            task -> tuples.map(_._2).sum
        }
    }
    val overallSuccessFuture: Future[Int] = taskSuccessesFuture map {
      xs =>
        val max = if (xs.isEmpty) 0 else xs.values.max
        max withEffect {
          n =>
            if (n == 0) {
              throw new DefunctBeamException("Tasks are all gone: %s" format tasks.map(_.id).mkString(", "))
            }
        }
    }
    overallSuccessFuture
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
