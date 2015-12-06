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

import com.fasterxml.jackson.core.JsonGenerator
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.net.curator.DiscoConfig
import com.metamx.common.scala.timekeeper.SystemTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.ClusteredBeam
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.beam.MergingPartitioningBeam
import com.metamx.tranquility.finagle.BeamService
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.partition.GenericTimeAndDimsPartitioner
import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.typeclass.JavaObjectWriter
import com.metamx.tranquility.typeclass.JsonWriter
import com.metamx.tranquility.typeclass.ObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.finagle.Service
import io.druid.data.input.impl.TimestampSpec
import java.{lang => jl}
import java.{util => ju}
import org.apache.curator.framework.CuratorFramework
import org.joda.time.DateTime
import org.joda.time.Interval
import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

/**
  * Builds Beams or Finagle services that send events to the Druid indexing service.
  *
  * {{{
  * val curator = CuratorFrameworkFactory.newClient("localhost:2181", new BoundedExponentialBackoffRetry(100, 30000, 30))
  * curator.start()
  * val dataSource = "foo"
  * val dimensions = Seq("bar")
  * val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))
  * val service = DruidBeams
  *   .builder[Map[String, Any]](eventMap => new DateTime(eventMap("timestamp")))
  *   .curator(curator)
  *   .discoveryPath("/test/discovery")
  *   .location(DruidLocation(new DruidEnvironment("druid:local:indexer", "druid:local:firehose:%s"), dataSource))
  *   .rollup(DruidRollup(dimensions, aggregators, QueryGranularity.MINUTE))
  *   .tuning(new ClusteredBeamTuning(Granularity.HOUR, 10.minutes, 1, 1))
  *   .buildService()
  * val future = service(Seq(Map("timestamp" -> "2010-01-02T03:04:05.678Z", "bar" -> "hey", "baz" -> 3)))
  * println("result = %s" format Await.result(future))
  * }}}
  *
  * Your event type (in this case, `Map[String, Any]`) must be serializable via Jackson to JSON that Druid can
  * understand. If Jackson is not an appropriate choice, you can provide an ObjectWriter via `.objectWriter(...)`.
  */
object DruidBeams
{
  private val DefaultTimestampSpec = new TimestampSpec("timestamp", "iso", null)

  /**
    * Start a builder for a particular event type.
    *
    * @param timeFn time extraction function for this event type
    * @tparam EventType the event type
    * @return a new builder
    */
  def builder[EventType](timeFn: EventType => DateTime) = {
    new Builder[EventType](
      new BuilderConfig(
        _timestamper = Some(
          new Timestamper[EventType]
          {
            override def timestamp(a: EventType) = timeFn(a)
          }
        )
      )
    )
  }

  /**
    * Start a builder for a particular event type.
    *
    * @param timestamper time extraction object for this event type
    * @tparam EventType the event type
    * @return a new builder
    */
  def builder[EventType]()(implicit timestamper: Timestamper[EventType]) = {
    new Builder[EventType](new BuilderConfig(_timestamper = Some(timestamper)))
  }

  class Builder[EventType] private[druid](config: BuilderConfig[EventType])
  {
    /**
      * Provide a CuratorFramework instance that will be used for Tranquility's internal coordination. Required.
      *
      * If you do not provide your own [[Builder.finagleRegistry]], this will be used for service discovery as well.
      *
      * @param curator curator
      * @return this instance
      */
    def curator(curator: CuratorFramework) = new Builder[EventType](config.copy(_curator = Some(curator)))

    /**
      * Provide a znode used for Druid service discovery. Optional, defaults to "/druid/discovery".
      *
      * If you do not provide a [[Builder.finagleRegistry]], this will be used along with your provided
      * CuratorFramework to locate Druid services. If you do provide a FinagleRegistry, this option will not be used.
      *
      * @param path discovery znode
      * @return this instance
      */
    def discoveryPath(path: String) = new Builder[EventType](config.copy(_discoveryPath = Some(path)))

    /**
      * Provide tunings for coordination of Druid task creation. Optional, see
      * [[com.metamx.tranquility.beam.ClusteredBeamTuning$]] for defaults.
      *
      * These influence how and when Druid tasks are created.
      *
      * @param tuning tuning object
      * @return this instance
      */
    def tuning(tuning: ClusteredBeamTuning) = new Builder[EventType](config.copy(_tuning = Some(tuning)))

    /**
      * Provide tunings for the Druid realtime engine. Optional, see [[DruidTuning]] for defaults.
      *
      * These will be passed along to the Druid tasks.
      *
      * @param druidTuning tuning object
      * @return this instance
      */
    def druidTuning(druidTuning: DruidTuning) = new Builder[EventType](config.copy(_druidTuning = Some(druidTuning)))

    /**
      * Provide the location of your Druid dataSource. Required.
      *
      * This will be used to determine the service name of your Druid overlord and tasks, and to choose which
      * dataSource to write to.
      *
      * @param location location object
      * @return this instance
      */
    def location(location: DruidLocation) = new Builder[EventType](config.copy(_location = Some(location)))

    /**
      * Provide rollup (dimensions, aggregators, query granularity). Required.
      *
      * @param rollup rollup object
      * @return this instance
      */
    def rollup(rollup: DruidRollup) = new Builder[EventType](config.copy(_rollup = Some(rollup)))

    /**
      * Provide a Druid timestampSpec. Optional, defaults to timestampColumn "timestamp" and timestampFormat "iso".
      *
      * Druid will use this to parse the timestamp of your serialized events.
      *
      * @param timestampSpec timestampSpec object
      * @return this instance
      */
    def timestampSpec(timestampSpec: TimestampSpec) = {
      new Builder[EventType](config.copy(_timestampSpec = Some(timestampSpec)))
    }

    /**
      * Provide the ZooKeeper znode that should be used for Tranquility's internal coordination. Optional, defaults
      * to "/tranquility/beams".
      *
      * @param path the path
      * @return this instance
      */
    def clusteredBeamZkBasePath(path: String) = {
      new Builder[EventType](config.copy(_clusteredBeamZkBasePath = Some(path)))
    }

    /**
      * Provide the identity of this Beam, which will be used for coordination. Optional, defaults to your overlord
      * service discovery key + "/" + your dataSource.
      *
      * All beams with the same identity coordinate with each other on Druid tasks.
      *
      * @param ident ident string
      * @return this instance
      */
    def clusteredBeamIdent(ident: String) = new Builder[EventType](config.copy(_clusteredBeamIdent = Some(ident)))

    /**
      * Provide tunings for communication with Druid tasks. Optional, see [[DruidBeamConfig]] for defaults.
      *
      * @param beamConfig beam config tunings
      * @return this instance
      */
    def druidBeamConfig(beamConfig: DruidBeamConfig) = {
      new Builder[EventType](config.copy(_druidBeamConfig = Some(beamConfig)))
    }

    /**
      * Provide an emitter that will be used to emit alerts. By default, alerts are emitted through a logger.
      *
      * @param emitter an emitter
      * @return this instance
      */
    def emitter(emitter: ServiceEmitter) = new Builder[EventType](config.copy(_emitter = Some(emitter)))

    /**
      * Provide a FinagleRegistry that will be used to generate clients for your Overlord and Druid tasks. Optional,
      * by default this is built based on [[Builder.curator]] and [[Builder.discoveryPath]].
      *
      * @param registry a registry
      * @return this instance
      */
    def finagleRegistry(registry: FinagleRegistry) = {
      new Builder[EventType](config.copy(_finagleRegistry = Some(registry)))
    }

    /**
      * Provide a Timekeeper that will be used to determine what time it is for purposes of judging the windowPeriod.
      * Optional, by default this uses wall clock time. This is mostly useful for tests, as in real-world use it
      * is expected that you will be using wall clock time.
      *
      * @param timekeeper a timekeeper
      * @return this instance
      */
    def timekeeper(timekeeper: Timekeeper) = new Builder[EventType](config.copy(_timekeeper = Some(timekeeper)))

    /**
      * Provide a function that decorates each per-partition, per-interval beam. Optional, by default there is no
      * decoration. This is often used for gathering metrics.
      *
      * @param f function
      * @return this instance
      */
    def beamDecorateFn(f: (Interval, Int) => Beam[EventType] => Beam[EventType]) = {
      new Builder(config.copy(_beamDecorateFn = Some(f)))
    }

    /**
      * Provide a function that merges beams for different partitions. Optional, by default this creates a
      * [[com.metamx.tranquility.beam.MergingPartitioningBeam]] around your [[Builder.partitioner]]. You cannot
      * provide both a beamMergeFn and a partitioner.
      *
      * @param f function
      * @return this instance
      */
    def beamMergeFn(f: Seq[Beam[EventType]] => Beam[EventType]) = {
      if (config._partitioner.nonEmpty) {
        throw new IllegalStateException("Cannot set both 'beamMergeFn' and 'partitioner'")
      }
      new Builder[EventType](config.copy(_beamMergeFn = Some(f)))
    }

    /**
      * Provide a partitioner that determines how to route events when you have more than one Druid partition. By
      * default this uses time-and-dimensions based partitioning for simple Map types, and uses hashCode for custom
      * types. If you are using a custom type, you need to provide your own partitioner or beamMergeFn to get
      * optimal rollup. You cannot provide both a beamMergeFn and a partitioner.
      *
      * @param partitioner a partitioner
      * @return this instance
      */
    def partitioner(partitioner: Partitioner[EventType]) = {
      if (config._beamMergeFn.nonEmpty) {
        throw new IllegalStateException("Cannot set both 'beamMergeFn' and 'partitioner'")
      }
      new Builder[EventType](config.copy(_partitioner = Some(partitioner)))
    }

    /**
      * Provide extra information that will be emitted along with alerts. Optional, by default this is empty.
      *
      * @param d extra information
      * @return this instance
      */
    def alertMap(d: Dict) = new Builder[EventType](config.copy(_alertMap = Some(d)))

    @deprecated("use .objectWriter(...)", "0.2.21")
    def eventWriter(writer: ObjectWriter[EventType]) = {
      new Builder[EventType](config.copy(_objectWriter = Some(writer)))
    }

    /**
      * Provide a serializer for your event type. Optional, by default this uses a Jackson ObjectMapper.
      *
      * This method is designed use for Scala users.
      *
      * @param writer the serializer
      * @return this instance
      */
    def objectWriter(writer: ObjectWriter[EventType]) = {
      new Builder[EventType](config.copy(_objectWriter = Some(writer)))
    }

    /**
      * Provide a serializer for your event type. Optional, by default this uses a Jackson ObjectMapper.
      *
      * @param writer the serializer
      * @return this instance
      */
    def objectWriter(writer: JavaObjectWriter[EventType]) = {
      new Builder[EventType](config.copy(_objectWriter = Some(ObjectWriter.wrap(writer))))
    }

    def eventTimestamped(timeFn: EventType => DateTime) = {
      new Builder[EventType](
        config.copy(
          _timestamper = Some(
            new Timestamper[EventType]
            {
              def timestamp(a: EventType) = timeFn(a)
            }
          )
        )
      )
    }

    /**
      * Build a Beam using this DruidBeams builder.
      * @return a beam
      */
    def buildBeam(): Beam[EventType] = {
      val things = config.buildAll()
      implicit val eventTimestamped = things.timestamper
      val lifecycle = new Lifecycle
      val indexService = new IndexService(
        things.location.environment,
        things.druidBeamConfig,
        things.finagleRegistry,
        things.druidObjectMapper,
        lifecycle
      )
      val druidBeamMaker = new DruidBeamMaker[EventType](
        things.druidBeamConfig,
        things.location,
        things.tuning,
        things.druidTuning,
        things.rollup,
        things.timestampSpec,
        things.finagleRegistry,
        indexService,
        things.emitter,
        things.objectWriter
      )
      val clusteredBeam = new ClusteredBeam(
        things.clusteredBeamZkBasePath,
        things.clusteredBeamIdent,
        things.tuning,
        things.curator,
        things.emitter,
        things.timekeeper,
        things.scalaObjectMapper,
        druidBeamMaker,
        things.beamDecorateFn,
        things.beamMergeFn,
        things.alertMap
      )
      new Beam[EventType]
      {
        def propagate(events: Seq[EventType]) = clusteredBeam.propagate(events)

        def close() = clusteredBeam.close() map (_ => lifecycle.stop())

        override def toString = clusteredBeam.toString
      }
    }

    /**
      * Build a Finagle Service using this DruidBeams builder. This simply wraps the beam.
      * @return a service
      */
    def buildService(): Service[Seq[EventType], Int] = {
      new BeamService(buildBeam())
    }

    /**
      * Build a Finagle Service using this DruidBeams builder, designed for Java users. This simply wraps the beam.
      * @return a service
      */
    def buildJavaService(): Service[ju.List[EventType], jl.Integer] = {
      val delegate = buildService()
      Service.mk((xs: ju.List[EventType]) => delegate(xs.asScala).map(Int.box))
    }
  }

  private case class BuilderConfig[EventType](
    _curator: Option[CuratorFramework] = None,
    _discoveryPath: Option[String] = None,
    _tuning: Option[ClusteredBeamTuning] = None,
    _druidTuning: Option[DruidTuning] = None,
    _location: Option[DruidLocation] = None,
    _rollup: Option[DruidRollup] = None,
    _timestampSpec: Option[TimestampSpec] = None,
    _clusteredBeamZkBasePath: Option[String] = None,
    _clusteredBeamIdent: Option[String] = None,
    _druidBeamConfig: Option[DruidBeamConfig] = None,
    _emitter: Option[ServiceEmitter] = None,
    _finagleRegistry: Option[FinagleRegistry] = None,
    _timekeeper: Option[Timekeeper] = None,
    _beamDecorateFn: Option[(Interval, Int) => Beam[EventType] => Beam[EventType]] = None,
    _partitioner: Option[Partitioner[EventType]] = None,
    _beamMergeFn: Option[Seq[Beam[EventType]] => Beam[EventType]] = None,
    _alertMap: Option[Dict] = None,
    _objectWriter: Option[ObjectWriter[EventType]] = None,
    _timestamper: Option[Timestamper[EventType]] = None
  )
  {
    def buildAll() = new {
      val scalaObjectMapper       = Jackson.newObjectMapper()
      val druidObjectMapper       = DruidGuicer.objectMapper
      val curator                 = _curator getOrElse {
        throw new IllegalArgumentException("Missing 'curator'")
      }
      val tuning                  = _tuning getOrElse {
        ClusteredBeamTuning()
      }
      val druidTuning             = _druidTuning getOrElse {
        DruidTuning()
      }
      val location                = _location getOrElse {
        throw new IllegalArgumentException("Missing 'location'")
      }
      val rollup                  = _rollup getOrElse {
        throw new IllegalArgumentException("Missing 'rollup'")
      }
      val timestampSpec           = _timestampSpec getOrElse {
        DefaultTimestampSpec
      }
      val clusteredBeamZkBasePath = _clusteredBeamZkBasePath getOrElse "/tranquility/beams"
      val clusteredBeamIdent      = _clusteredBeamIdent getOrElse {
        "%s/%s" format(location.environment.indexService, location.dataSource)
      }
      val druidBeamConfig         = _druidBeamConfig getOrElse DruidBeamConfig()
      val emitter                 = _emitter getOrElse {
        val em = new ServiceEmitter(
          "tranquility",
          "localhost",
          new LoggingEmitter(new Logger(classOf[LoggingEmitter]), LoggingEmitter.Level.INFO, scalaObjectMapper)
        )
        em.start()
        em
      }
      val finagleRegistry         = _finagleRegistry getOrElse {
        val discoveryPath = _discoveryPath getOrElse "/druid/discovery"
        val disco = new Disco(
          curator,
          new DiscoConfig
          {
            def discoAnnounce = None

            def discoPath = discoveryPath
          }
        )
        new FinagleRegistry(FinagleRegistryConfig(), disco)
      }
      val timekeeper              = _timekeeper getOrElse new SystemTimekeeper
      val beamDecorateFn          = _beamDecorateFn getOrElse {
        (interval: Interval, partition: Int) => (beam: Beam[EventType]) => beam
      }
      val alertMap                = _alertMap getOrElse Map.empty
      val objectWriter            = _objectWriter getOrElse {
        new JsonWriter[EventType]
        {
          override protected def viaJsonGenerator(a: EventType, jg: JsonGenerator): Unit = {
            scalaObjectMapper.writeValue(jg, a)
          }
        }
      }
      val timestamper             = _timestamper getOrElse {
        throw new IllegalArgumentException("WTF?! Should have had a Timestamperable event...")
      }
      val beamMergeFn             = _beamMergeFn getOrElse {
        val partitioner = _partitioner getOrElse {
          GenericTimeAndDimsPartitioner.create(
            timestamper,
            timestampSpec,
            rollup
          )
        }
        (beams: Seq[Beam[EventType]]) => {
          new MergingPartitioningBeam[EventType](partitioner, beams.toIndexedSeq)
        }
      }
    }
  }

}
