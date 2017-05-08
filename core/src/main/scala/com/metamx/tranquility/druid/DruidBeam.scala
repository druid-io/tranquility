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

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.DefunctBeamException
import com.metamx.tranquility.beam.SendResult
import com.metamx.tranquility.finagle._
import com.metamx.tranquility.typeclass.ObjectWriter
import com.twitter.io.Buf
import com.twitter.util._

/**
  * A Beam that writes all messages to a fixed set of Druid tasks.
  */
class DruidBeam[A](
  private[druid] val interval: Interval,
  private[druid] val partition: Int,
  private[druid] val tasks: Seq[TaskPointer],
  location: DruidLocation,
  config: DruidBeamConfig,
  taskLocator: TaskLocator,
  indexService: IndexService,
  emitter: ServiceEmitter,
  objectWriter: ObjectWriter[A]
) extends Beam[A] with Logging with Closable
{
  private[this] val clients = Map(
    tasks map {
      task =>
        task ->
          new TaskClient(
            task,
            taskLocator.connect(task),
            location.dataSource,
            config.firehoseQuietPeriod,
            config.firehoseRetryPeriod,
            indexService,
            emitter
          )
    }: _*
  )

  override def sendAll(messages: Seq[A]): Seq[Future[SendResult]] = {
    val messagesWithPromises = Vector() ++ messages.map(message => (message, Promise[SendResult]()))

    // Messages grouped into chunks
    val messagesChunks: List[(Array[Byte], IndexedSeq[(A, Promise[SendResult])])] = messagesWithPromises
      .grouped(config.firehoseChunkSize)
      .map(xs => (objectWriter.batchAsBytes(xs.map(_._1)), xs))
      .toList

    for ((messagesChunkBytes, messagesChunk) <- messagesChunks) {
      // Try to send to all tasks, return "sent" if any of them accepted it.
      val taskResponses: Seq[Future[(TaskPointer, SendResult)]] = for {
        task <- tasks
        client <- clients.get(task) if client.active
      } yield {
        val messagePost = HttpPost(
          "/druid/worker/v1/chat/%s/push-events" format
            (location.environment.firehoseServicePattern format task.serviceKey)
        ) withEffect {
          req =>
            req.headerMap("Content-Type") = objectWriter.contentType
            req.headerMap("Content-Length") = messagesChunkBytes.length.toString
            req.content = Buf.ByteArray.Shared(messagesChunkBytes)
        }
        if (log.isTraceEnabled) {
          log.trace(
            "Sending %,d messages to task[%s], firehose[%s]: %s",
            messagesChunk.size,
            task.id,
            task.serviceKey,
            new String(messagesChunkBytes)
          )
        }
        client(messagePost) map {
          case Some(response) => task -> SendResult.Sent
          case None => task -> SendResult.Dropped
        }
      }

      // Get the SendResult for this chunk.
      val chunkResult: Future[SendResult] = Future.collect(taskResponses) map { responses =>
        responses collectFirst {
          case (taskPointer, taskResult) if taskResult.sent => taskResult
        } getOrElse {
          // Nothing failed (or else Future.collect would have returned a failed future) but also nothing sent.
          // This means all tasks must be gone.
          throw new DefunctBeamException("Tasks are all gone: %s" format tasks.map(_.id).mkString(", "))
        }
      }

      // Avoid become(chunkResult), for some reason it creates massive promise chains.
      chunkResult respond {
        case Return(result) => messagesChunk.foreach(_._2.setValue(result))
        case Throw(e) => messagesChunk.foreach(_._2.setException(e))
      }
    }

    messagesWithPromises.map(_._2)
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
