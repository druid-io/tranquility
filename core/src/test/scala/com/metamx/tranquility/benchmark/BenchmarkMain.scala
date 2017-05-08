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

package com.metamx.tranquility.benchmark

import com.github.nscala_time.time.Imports._
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.untyped._
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.BeamPacketizer
import com.metamx.tranquility.beam.BeamPacketizerListener
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.test.common.CuratorRequiringSuite
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.finagle.Resolver
import com.twitter.finagle.Service
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.util.NonFatal
import com.twitter.util.Return
import com.twitter.util.Throw
import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicLong
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.collection.Set

object BenchmarkMain extends Logging with CuratorRequiringSuite
{

  class OverlordService extends Service[http.Request, http.Response]
  {
    val StatusPath = """/druid/indexer/v1/task/(.*)/status""".r

    override def apply(request: http.Request): Future[http.Response] = {
      request.path match {
        case "/druid/indexer/v1/task" if request.method == Method.Post =>
          val bytes: Array[Byte] = Buf.ByteArray.Owned.extract(request.content)
          val content: Dict = Jackson.parse[Dict](bytes)
          val taskId = str(content("id"))
          Future.value(
            http.Response(request.version, http.Status.Ok) withEffect { response =>
              response.contentType = "application/json"
              response.content = Buf.ByteArray.Owned(Jackson.bytes(Dict("task" -> taskId)))
            }
          )

        case StatusPath(taskId) if request.method == Method.Get =>
          Future.value(
            http.Response(request.version, http.Status.Ok) withEffect { response =>
              response.contentType = "application/json"
              response.content = Buf.ByteArray.Owned(Jackson.bytes(Dict("status" -> "RUNNING")))
            }
          )

        case _ =>
          Future.value(http.Response(request.version, http.Status.NotFound))
      }
    }
  }

  class TaskService extends Service[http.Request, http.Response]
  {
    val PushPath = """/druid/worker/v1/chat/(.*)/push-events""".r

    override def apply(request: http.Request): Future[http.Response] = {
      request.path match {
        case PushPath(firehoseId) if request.method == Method.Post =>
          Future.value(http.Response(request.version, http.Status.Ok))

        case _ =>
          Future.value(http.Response(request.version, http.Status.NotFound))
      }
    }
  }

  def main(args: Array[String]) {
    val finagleRegistry = new FinagleRegistry(
      FinagleRegistryConfig(),
      Nil
    ) with Logging
    {
      override def addResolver(resolver: Resolver): Unit = {
        // do nothing
      }

      override def schemes: Set[String] = Set("disco")

      override def connect(scheme: String, name: String): Service[Request, Response] = {
        require(scheme == "disco", "expected scheme 'disco'")
        log.info(s"Checkout[$name]")
        name match {
          case "druid:overlord" => new OverlordService
          case x if x.startsWith("firehose:") => new TaskService
        }
      }
    }

    withLocalCurator { curator =>
      val configStream = getClass.getClassLoader.getResourceAsStream("benchmark.json")
      val configString = new String(ByteStreams.toByteArray(configStream), Charsets.UTF_8)
      val config: TranquilityConfig[PropertiesBasedConfig] = TranquilityConfig.read(
        new ByteArrayInputStream(
          configString
            .replaceAll("@ZKCONNECT@", curator.getZookeeperClient.getCurrentConnectionString)
            .getBytes(Charsets.UTF_8)
        )
      )
      val wikipediaConfig: DataSourceConfig[PropertiesBasedConfig] = config.getDataSource("wikipedia")
      val druidBeamsBuilder = {
        DruidBeams
          .fromConfig(config.getDataSource("wikipedia"))
          .curator(curator)
          .finagleRegistry(finagleRegistry)
      }

      benchmarkTranquilizer(druidBeamsBuilder.buildTranquilizer(wikipediaConfig.tranquilizerBuilder()))
//      benchmarkBeamPacketizer(
//        druidBeamsBuilder.buildBeam(),
//        wikipediaConfig.propertiesBasedConfig.tranquilityMaxBatchSize,
//        wikipediaConfig.propertiesBasedConfig.tranquilityMaxPendingBatches
//      )
    }
  }

  def benchmarkTranquilizer(sender: Tranquilizer[java.util.Map[String, AnyRef]]): Unit =
  {
    var startTime: DateTime = null
    val warmCount = 450000L
    val count = 5000000L
    val printEvery = 25000L
    val sent = new AtomicLong
    val dropped = new AtomicLong
    val failed = new AtomicLong

    val obj = Map[String, AnyRef](
      "timestamp" -> DateTime.now.toString(),
      "page" -> "foo",
      "added" -> Int.box(1)
    )

    try {
      sender.start()

      log.info("Warming up...")

      for (i <- 0L until warmCount) {
        sender.send(obj.asJava)
      }

      sender.flush()
      System.gc()

      log.info(s"Warm up done. Sending $count messages.")

      startTime = DateTime.now

      for (i <- 0L until count) {
        sender.send(obj.asJava) respond {
          case Return(_) =>
            val sentCount = sent.incrementAndGet()
            if (sentCount % printEvery == 0) {
              println(s"Sent $sentCount/$count messages (${(sentCount.toDouble / count * 100).toLong}%).")
            }
          case Throw(e: MessageDroppedException) => dropped.incrementAndGet()
          case Throw(e) => failed.incrementAndGet()
        }
      }
    }
    catch {
      case NonFatal(e) =>
        log.warn(e, "Main loop died!")
    }
    finally {
      sender.flush()
      sender.stop()
    }

    val elapsed = (startTime to DateTime.now).millis
    println(
      s"Sent ${sent.get()}/$count messages in ${elapsed}ms " +
        s"(${sent.get() * 1000 / elapsed} messages/sec) " +
        s"(${dropped.get()} dropped, ${failed.get()} failed)"
    )
  }

  def benchmarkBeamPacketizer(
    beam: Beam[java.util.Map[String, AnyRef]],
    batchSize: Int,
    maxPendingBatches: Int
  ): Unit =
  {
    var startTime: DateTime = null
    val warmCount = 150000L
    val count = 2000000L
    val printEvery = 10000L
    val sent = new AtomicLong
    val dropped = new AtomicLong
    val failed = new AtomicLong

    val obj = Map[String, AnyRef](
      "timestamp" -> DateTime.now.toString(),
      "page" -> "foo",
      "added" -> Int.box(1)
    )

    val sender = new BeamPacketizer[java.util.Map[String, AnyRef]](
      beam,
      new BeamPacketizerListener[java.util.Map[String, AnyRef]]
      {
        override def fail(e: Throwable, message: java.util.Map[String, AnyRef]): Unit = {
          failed.incrementAndGet()
        }

        override def ack(message: java.util.Map[String, AnyRef]): Unit = {
          val sentCount = sent.incrementAndGet()
          if (sentCount % printEvery == 0) {
            println(s"Sent $sentCount/$count messages (${(sentCount.toDouble / count * 100).toLong}%).")
          }
        }
      },
      batchSize,
      maxPendingBatches
    )

    try {
      sender.start()

      log.info("Warming up...")

      for (i <- 0L until warmCount) {
        sender.send(obj.asJava)
      }

      sender.flush()
      require(sent.get() == warmCount)
      sent.set(0)
      System.gc()

      log.info(s"Warm up done. Sending $count messages.")

      startTime = DateTime.now

      for (i <- 0L until count) {
        sender.send(obj.asJava)
      }
    }
    catch {
      case NonFatal(e) =>
        log.warn(e, "Main loop died!")
    }
    finally {
      sender.flush()
      sender.close()
    }

    val elapsed = (startTime to DateTime.now).millis
    println(
      s"Sent ${sent.get()}/$count messages in ${elapsed}ms " +
        s"(${sent.get() * 1000 / elapsed} messages/sec) " +
        s"(${dropped.get()} dropped, ${failed.get()} failed)"
    )
  }
}
