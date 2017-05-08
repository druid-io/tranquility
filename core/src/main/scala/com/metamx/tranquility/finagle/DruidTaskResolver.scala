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

package com.metamx.tranquility.finagle

import com.github.nscala_time.time.Imports._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Logging
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.tranquility.druid.IndexService
import com.twitter.finagle.Addr
import com.twitter.finagle.Resolver
import com.twitter.util._
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import scala.util.Random
import scala.util.control.NonFatal

class DruidTaskResolver(
  indexService: IndexService,
  timekeeper: Timekeeper,
  pollPeriod: Period
) extends Resolver with Logging
{
  private val pollExecutor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat(s"DruidTaskResolver[$indexService]").setDaemon(true).build()
  )

  private val started      = new AtomicBoolean
  private val taskNumberer = new AtomicLong

  private val lock     = new AnyRef
  private var nextPoll = 0L
  private var tasks    = Map[Long, (String, AtomicReference[Addr], Updatable[Addr])]()

  private def ensureStarted() {
    if (started.compareAndSet(false, true)) {
      log.info(s"Starting poll thread for indexService[$indexService], pollPeriod[$pollPeriod].")
      pollExecutor.submit(
        new Runnable
        {
          override def run(): Unit = {
            while (!Thread.currentThread().isInterrupted) {
              try {
                val tasksSnapshot: Map[Long, (String, AtomicReference[Addr], Updatable[Addr])] = lock.synchronized {
                  while (timekeeper.now.getMillis < nextPoll) {
                    val waitMillis = nextPoll - timekeeper.now.getMillis
                    if (waitMillis > 0) {
                      lock.wait(waitMillis)
                    }
                  }
                  tasks
                }

                val now = timekeeper.now
                val baseWait = now.plus(pollPeriod).getMillis - now.getMillis
                val fuzzyWait = (math.max(1 + 0.25 * Random.nextGaussian, 0) * baseWait).toLong
                nextPoll = now.getMillis + fuzzyWait

                val running = Await.result(indexService.runningTasks())

                log.debug(s"Polled[$indexServiceKey] and found ${running.size} tasks " +
                  s"(${running.count(_._2.isBound)} bound).")

                for ((taskNumber, (taskId, addr, updatable)) <- tasksSnapshot) {
                  val runningHostPort = running.get(taskId)
                  val runningAddr: Addr = runningHostPort.map(_.toAddr).getOrElse(Addr.Neg)
                  if (addr.get() != runningAddr) {
                    log.info(s"Updating location for task[$taskId] (#$taskNumber) to[$runningAddr].")
                    addr.set(runningAddr)
                    updatable.update(runningAddr)
                  }
                }
              }
              catch {
                case NonFatal(e) =>
                  log.warn(e, s"Poll failed, trying again at[${new DateTime(nextPoll)}].")
              }
            }
          }
        }
      )
    }
  }

  override val scheme = DruidTaskResolver.mkscheme(indexService.key)

  override def bind(taskId: String) = {
    val taskNumber = taskNumberer.incrementAndGet()

    Var.async[Addr](Addr.Pending) {
      updatable =>
        log.info(s"Started monitoring task[$taskId] (#$taskNumber).")

        lock.synchronized {
          nextPoll = 0L
          tasks = tasks + (taskNumber ->(taskId, new AtomicReference[Addr], updatable))
          lock.notifyAll()
        }

        ensureStarted()
        new Closable
        {
          override def close(deadline: Time): Future[Unit] = {
            lock.synchronized {
              log.info(s"Stopped monitoring task[$taskId] (#$taskNumber).")
              tasks = tasks - taskNumber
              Future.Done
            }
          }
        }
    }
  }

  def indexServiceKey: String = indexService.key
}

object DruidTaskResolver
{
  def mkscheme(indexServiceKey: String) = s"druidTask!$indexServiceKey"
}
