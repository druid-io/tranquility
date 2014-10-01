package com.metamx.tranquility.test.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.{CharStreams, Files}
import com.google.inject.Injector
import com.metamx.common.scala.concurrent._
import com.metamx.common.scala.untyped._
import com.metamx.common.scala.{Jackson, Logging}
import io.druid.cli.{CliBroker, CliOverlord, GuiceRunnable}
import io.druid.guice.GuiceInjectors
import io.druid.query.Query
import io.druid.server.ClientQuerySegmentWalker
import java.io.{File, InputStreamReader}
import org.apache.curator.framework.CuratorFramework
import org.scalatest.FunSuite
import scala.util.Random

trait DruidIntegrationSuite extends Logging with CuratorRequiringSuite
{
  self: FunSuite =>

  trait DruidServerHandle
  {
    def injector: Injector

    def close()
  }

  def writeConfig(configResource: String, replacements: Map[String, String]): File = {
    val stream = ClassLoader.getSystemResourceAsStream(configResource)
    val text = CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8))
    val configFile = File.createTempFile("runtime-", ".properties")
    Files.write(replacements.foldLeft(text)((a, kv) => a.replace(kv._1, kv._2)), configFile, Charsets.UTF_8)
    configFile.deleteOnExit()
    configFile
  }

  def spawnDruidServer[A <: GuiceRunnable : ClassManifest](configFile: File): DruidServerHandle = {
    val server = classManifest[A].erasure.newInstance().asInstanceOf[A]
    val serverName = server.getClass.getName
    // Would be better to have a way to easily pass Properties into the startup injector.
    System.setProperty("druid.properties.file", configFile.toString)
    server.configure(GuiceInjectors.makeStartupInjector())
    val _injector = server.makeInjector()
    val lifecycle = server.initLifecycle(_injector)
    System.clearProperty("druid.properties.file")
    log.info("Server started up: %s", serverName)
    val thread = loggingThread {
      try {
        lifecycle.join()
      }
      catch {
        case e: Throwable =>
          log.error(e, "Failed to run server: %s", serverName)
          throw e
      }
    }
    thread.start()
    new DruidServerHandle
    {
      def injector = _injector

      def close() {
        try {
          lifecycle.stop()
        }
        catch {
          case e: Throwable =>
            log.error(e, "Failed to stop lifecycle for server: %s", serverName)
        }
        thread.interrupt()
      }
    }
  }

  def withBroker[A](curator: CuratorFramework)(f: DruidServerHandle => A): A = {
    // Randomize, but don't bother checking for conflicts
    val overlordPort = new Random().nextInt(100) + 28100
    val configFile = writeConfig(
      "druid-broker.properties",
      Map(
        ":DRUIDPORT:" -> overlordPort.toString,
        ":ZKCONNECT:" -> curator.getZookeeperClient.getCurrentConnectionString
      )
    )
    val handle = spawnDruidServer[CliBroker](configFile)
    try {
      f(handle)
    }
    finally {
      handle.close()
    }
  }

  def withOverlord[A](curator: CuratorFramework)(f: DruidServerHandle => A): A = {
    // Randomize, but don't bother checking for conflicts
    val overlordPort = new Random().nextInt(100) + 28200
    val configFile = writeConfig(
      "druid-overlord.properties",
      Map(
        ":DRUIDPORT:" -> overlordPort.toString,
        ":DRUIDFORKPORT:" -> (overlordPort + 1).toString,
        ":ZKCONNECT:" -> curator.getZookeeperClient.getCurrentConnectionString
      )
    )
    val handle = spawnDruidServer[CliOverlord](configFile)
    try {
      f(handle)
    }
    finally {
      handle.close()
    }
  }

  def withDruidStack[A](f: (CuratorFramework, DruidServerHandle, DruidServerHandle) => A): A = {
    withLocalCurator {
      curator =>
        curator.create().forPath("/beams")
        withBroker(curator) {
          broker =>
            withOverlord(curator) {
              overlord =>
                f(curator, broker, overlord)
            }
        }
    }
  }

  def assertQueryResults(broker: DruidServerHandle, query: Query[_], expected: Seq[Dict]) {
    val walker = broker.injector.getInstance(classOf[ClientQuerySegmentWalker])
    val brokerObjectMapper = broker.injector.getInstance(classOf[ObjectMapper])

    var got: Seq[Dict] = null
    val start = System.currentTimeMillis()
    while (got != expected && System.currentTimeMillis() < start + 300000L) {
      got = Jackson.parse[Seq[Dict]](brokerObjectMapper.writeValueAsBytes(query.run(walker)))
      val gotAsString = got.toString match {
        case x if x.size > 1024 => x.take(1024) + " ..."
        case x => x
      }
      if (got != expected) {
        log.info("Query result[%s] != expected result[%s], waiting a bit...", gotAsString, expected)
        Thread.sleep(500)
      }
    }
    assert(got === expected)
  }

}
