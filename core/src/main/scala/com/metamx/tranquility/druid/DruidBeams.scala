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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import com.metamx.common.logger.Logger
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.net.curator.Disco
import com.metamx.common.scala.net.curator.DiscoConfig
import com.metamx.common.scala.timekeeper.SystemTimekeeper
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.common.scala.untyped.dict
import com.metamx.common.scala.untyped.normalizeJava
import com.metamx.emitter.core.LoggingEmitter
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.beam.Beam
import com.metamx.tranquility.beam.ClusteredBeam
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.beam.MergingPartitioningBeam
import com.metamx.tranquility.beam.MessageHolder
import com.metamx.tranquility.beam.TransformingBeam
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.druid.input.InputRowObjectWriter
import com.metamx.tranquility.druid.input.InputRowPartitioner
import com.metamx.tranquility.druid.input.InputRowTimestamper
import com.metamx.tranquility.druid.input.ThreadLocalInputRowParser
import com.metamx.tranquility.finagle.BeamService
import com.metamx.tranquility.finagle.DruidTaskResolver
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.finagle.FinagleRegistryConfig
import com.metamx.tranquility.partition.MapPartitioner
import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.metamx.tranquility.typeclass.DefaultJsonWriter
import com.metamx.tranquility.typeclass.JavaObjectWriter
import com.metamx.tranquility.typeclass.ObjectWriter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.finagle.Service
import io.druid.data.input.InputRow
import io.druid.data.input.impl.DimensionSchema
import io.druid.data.input.impl.DimensionSchema.ValueType
import io.druid.data.input.impl.InputRowParser
import io.druid.data.input.impl.MapInputRowParser
import io.druid.data.input.impl.StringInputRowParser
import io.druid.data.input.impl.TimestampSpec
import io.druid.segment.realtime.FireDepartment
import java.nio.ByteBuffer
import java.{lang => jl}
import java.{util => ju}
import javax.ws.rs.core.MediaType
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scala_tools.time.Imports._
import scala.collection.JavaConverters._
import scala.language.reflectiveCalls
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

/**
  * Builds Beams or Finagle services that send events to the Druid indexing service.
  *
  * {{{
  * val curator = CuratorFrameworkFactory.newClient("localhost:2181", new BoundedExponentialBackoffRetry(100, 30000, 30))
  * curator.start()
  * val dataSource = "foo"
  * val dimensions = Seq("bar")
  * val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))
  * val isRollup = true
  * val sender = DruidBeams
  *   .builder[Map[String, Any]](eventMap => new DateTime(eventMap("timestamp")))
  *   .curator(curator)
  *   .discoveryPath("/test/discovery")
  *   .location(DruidLocation(new DruidEnvironment("druid:local:indexer", "druid:local:firehose:%s"), dataSource))
  *   .rollup(DruidRollup(dimensions, aggregators, QueryGranularities.MINUTE, isRollup))
  *   .tuning(new ClusteredBeamTuning(Granularity.HOUR, 10.minutes, 1, 1))
  *   .buildTranquilizer()
  * val future = sender.send(Map("timestamp" -> "2010-01-02T03:04:05.678Z", "bar" -> "hey", "baz" -> 3))
  * println("result = %s" format Await.result(future))
  * }}}
  *
  * Your event type (in this case, `Map[String, Any]`) must be serializable via Jackson to JSON that Druid can
  * understand. If Jackson is not an appropriate choice, you can provide an ObjectWriter via `.objectWriter(...)`.
  */
object DruidBeams
{
  private val DefaultScalaObjectMapper = Jackson.newObjectMapper()
  private val DefaultTimestampSpec     = new TimestampSpec("timestamp", "iso", null)

  val DefaultZookeeperPath     = "/tranquility/beams"

  /**
    * Start a builder for Java Maps based on a particular Tranquility dataSourceConfig. Not all of the realtime spec
    * in the config is used, but we do translate as much as possible into DruidBeams configurations. The builder
    * generated by this method will already have a tuning, druidTuning, location, rollup, objectWriter, timestampSpec,
    * partitions, replicants, and druidBeamConfig set.
    *
    * @param config Tranquility dataSource config
    * @return new builder
    */
  def fromConfig(
    config: DataSourceConfig[_ <: PropertiesBasedConfig]
  ): Builder[ju.Map[String, AnyRef], MessageHolder[InputRow]] =
  {
    fromConfig(config, typeTag[ju.Map[String, AnyRef]])
  }

  /**
    * Start an InputRow-based builder for a particular type based on a particular Tranquility dataSourceConfig. Not all
    * of the realtime spec in the config is used, but we do translate as much as possible into DruidBeams
    * configurations. The builder generated by this method will already have a tuning, druidTuning, location, rollup,
    * objectWriter, timestampSpec, partitions, replicants, and druidBeamConfig set.
    *
    * Only certain types are allowed. If you need another type, use the 3-arg fromConfig method. Supported types are:
    *
    * - {{{java.util.Map<String, Object>}}} (uses Druid Map Parser)
    * - {{{scala.collection.Map<String, Any>}}} (uses Druid Map Parser)
    * - {{{byte[]}}} (uses Druid String Parser)
    * - {{{String}}} (uses Druid String Parser)
    * - {{{java.nio.ByteBuffer}}} (uses Druid String Parser)
    * - {{{io.druid.data.input.InputRow}}} (does not use a Parser)
    *
    * This is private[tranquility] because I'm not sure yet if this is a good API to expose publically.
    *
    * @param config Tranquility dataSource config
    * @return new builder
    */
  private[tranquility] def fromConfig[MessageType](
    config: DataSourceConfig[_ <: PropertiesBasedConfig],
    tag: TypeTag[MessageType]
  ): Builder[MessageType, MessageHolder[InputRow]] =
  {
    val inputFnFn: (DruidRollup, () => InputRowParser[_], TimestampSpec) => MessageType => MessageHolder[InputRow] = {
      (rollup: DruidRollup, mkparser: () => InputRowParser[_], timestampSpec: TimestampSpec) => {
        val timestamper = InputRowTimestamper.Instance
        val partitioner = new InputRowPartitioner(rollup.indexGranularity)
        val parseSpec = mkparser().getParseSpec
        val parseFn: MessageType => InputRow = tag match {
          case t if t == typeTag[String] =>
            val trialParser = mkparser()
            require(
              trialParser.isInstanceOf[StringInputRowParser],
              s"Expected StringInputRowParser, got[${trialParser.getClass.getName}]"
            )
            val threadLocalParser = new ThreadLocalInputRowParser(mkparser)
            msg => threadLocalParser.get().asInstanceOf[StringInputRowParser].parse(msg.asInstanceOf[String])

          case t if t == typeTag[ByteBuffer] =>
            val trialParser = mkparser()
            require(
              trialParser.isInstanceOf[InputRowParser[ByteBuffer]],
              s"Expected InputRowParser of ByteBuffer, got[${trialParser.getClass.getName}]"
            )
            val threadLocalParser = new ThreadLocalInputRowParser(mkparser)
            msg => threadLocalParser.get().asInstanceOf[InputRowParser[ByteBuffer]].parse(msg.asInstanceOf[ByteBuffer])

          case t if t == typeTag[Array[Byte]] =>
            val trialParser = mkparser()
            require(
              trialParser.isInstanceOf[InputRowParser[ByteBuffer]],
              s"Expected InputRowParser of ByteBuffer, got[${trialParser.getClass.getName}]"
            )
            val threadLocalParser = new ThreadLocalInputRowParser(mkparser)
            msg => threadLocalParser.get().asInstanceOf[InputRowParser[ByteBuffer]].parse(
              ByteBuffer.wrap(msg.asInstanceOf[Array[Byte]])
            )

          case t if t.tpe <:< typeTag[ju.Map[String, AnyRef]].tpe =>
            val threadLocalParser = new ThreadLocalInputRowParser(() => new MapInputRowParser(parseSpec))
            msg => threadLocalParser.get().parse(msg.asInstanceOf[java.util.Map[String, AnyRef]])

          case t if t.tpe <:< typeTag[collection.Map[String, Any]].tpe =>
            val threadLocalParser = new ThreadLocalInputRowParser(() => new MapInputRowParser(parseSpec))
            msg => threadLocalParser.get().parse(msg.asInstanceOf[Map[String, AnyRef]].asJava)

          case t if t.tpe <:< typeTag[InputRow].tpe =>
            msg => msg.asInstanceOf[InputRow]

          case t =>
            throw new IllegalArgumentException(s"No builtin implementation for type[${t.tpe.toString}]")
        }
        (obj: MessageType) => MessageHolder(parseFn(obj), timestamper, partitioner)
      }
    }
    val timestamperFn = (timestampSpec: TimestampSpec) => MessageHolder.Timestamper
    var builder: Builder[MessageType, MessageHolder[InputRow]] = fromConfigInternal(
      inputFnFn,
      timestamperFn,
      config
    ).partitioner(MessageHolder.Partitioner)

    // Need to tweak timestampSpec, objectWriter to work properly on MessageHolder.
    val newTimestampSpec = new TimestampSpec(
      builder.config._timestampSpec.get.getTimestampColumn,
      "millis",
      null
    )
    val (objectMapper, contentType) = objectMapperAndContentTypeForFormat(
      config.propertiesBasedConfig.serializationFormat
    )
    builder = builder
      .timestampSpec(newTimestampSpec)
      .objectWriter(
        MessageHolder.wrapObjectWriter(
          new InputRowObjectWriter(
            newTimestampSpec,
            builder.config._rollup.get.aggregators,
            builder.config._rollup.get.dimensions.spatialDimensions,
            objectMapper,
            contentType
          )
        )
      )
    builder
  }

  /**
    * Start a builder for a custom type based on a particular Tranquility dataSourceConfig. Not all of the realtime spec
    * in the config is used, but we do translate as much as possible into DruidBeams configurations. The builder
    * generated by this method will already have a tuning, druidTuning, location, rollup, objectWriter, timestampSpec,
    * partitions, replicants, and druidBeamConfig set.
    *
    * @param config       Tranquility dataSource config
    * @param timestamper  Timestamper for this type
    * @param objectWriter Serializer for this type
    * @return new builder
    */
  def fromConfig[MessageType](
    config: DataSourceConfig[_ <: PropertiesBasedConfig],
    timestamper: Timestamper[MessageType],
    objectWriter: ObjectWriter[MessageType]
  ): Builder[MessageType, MessageType] =
  {
    fromConfigInternal[MessageType, MessageType](
      (rollup: DruidRollup, mkparser: () => InputRowParser[_], timestampSpec: TimestampSpec) => identity,
      (timestampSpec: TimestampSpec) => timestamper,
      config
    ).objectWriter(objectWriter)
  }

  /**
    * Start a builder for a custom type based on a particular Tranquility dataSourceConfig. Not all of the realtime spec
    * in the config is used, but we do translate as much as possible into DruidBeams configurations. The builder
    * generated by this method will already have a curatorFactory, tuning, druidTuning, location, rollup, objectWriter,
    * timestampSpec, partitions, replicants, and druidBeamConfig set.
    *
    * @param config       Tranquility dataSource config
    * @param timestamper  Timestamper for this type
    * @param objectWriter Serializer for this type
    * @return new builder
    */
  def fromConfig[MessageType](
    config: DataSourceConfig[_ <: PropertiesBasedConfig],
    timestamper: Timestamper[MessageType],
    objectWriter: JavaObjectWriter[MessageType]
  ): Builder[MessageType, MessageType] =
  {
    fromConfigInternal[MessageType, MessageType](
      (rollup: DruidRollup, mkparser: () => InputRowParser[_], timestampSpec: TimestampSpec) => identity,
      (timestampSpec: TimestampSpec) => timestamper,
      config
    ).objectWriter(objectWriter)
  }

  private def fromConfigInternal[InputType, MessageType](
    inputFnFn: (DruidRollup, () => InputRowParser[_], TimestampSpec) => (InputType => MessageType),
    timestamperFn: TimestampSpec => Timestamper[MessageType],
    config: DataSourceConfig[_ <: PropertiesBasedConfig]
  ): Builder[InputType, MessageType] =
  {
    def j2s[A](xs: ju.List[A]): IndexedSeq[A] = xs match {
      case null => Vector.empty
      case _ => xs.asScala.toIndexedSeq
    }
    def j2sSet[A](xs: ju.Set[A]): Set[A] = xs match {
      case null => Set.empty
      case _ => xs.asScala.toSet
    }
    val environment = DruidEnvironment(config.propertiesBasedConfig.druidIndexingServiceName)
    // Don't require people to specify a needless ioConfig
    val fireDepartment = makeFireDepartment(config)
    val mkparser = () => makeFireDepartment(config).getDataSchema.getParser
    val parseSpec = fireDepartment.getDataSchema.getParser.getParseSpec
    val timestampSpec = parseSpec.getTimestampSpec
    val spatialDimensions = j2s(parseSpec.getDimensionsSpec.getSpatialDimensions) map {
      spatial =>
        spatial.getDims match {
          case null => SingleFieldDruidSpatialDimension(spatial.getDimName)
          case xs if xs.isEmpty => SingleFieldDruidSpatialDimension(spatial.getDimName)
          case xs => MultipleFieldDruidSpatialDimension(spatial.getDimName, xs.asScala)
        }
    }
    val rollup = DruidRollup(
      dimensions = parseSpec.getDimensionsSpec.getDimensions match {
        case null =>
          SchemalessDruidDimensions(
            j2sSet(parseSpec.getDimensionsSpec.getDimensionExclusions),
            spatialDimensions
          )
        case xs if xs.isEmpty =>
          SchemalessDruidDimensions(
            j2sSet(parseSpec.getDimensionsSpec.getDimensionExclusions),
            spatialDimensions
          )
        case _ =>
          SpecificDruidDimensions(
            j2s(parseSpec.getDimensionsSpec.getDimensions) filter { dimensionSchema =>
              // Spatial dimensions are handled as a special case, above
              dimensionSchema.getTypeName != DimensionSchema.SPATIAL_TYPE_NAME
            } map { dimensionSchema =>
              dimensionSchema.getValueType match {
                case ValueType.STRING => dimensionSchema.getName
                case other =>
                  throw new IllegalStateException("Dimensions of type[%s] are not supported" format other)
              }
            },
            spatialDimensions
          )
      },
      aggregators = fireDepartment.getDataSchema.getAggregators,
      indexGranularity = fireDepartment.getDataSchema.getGranularitySpec.getQueryGranularity,
      isRollup = fireDepartment.getDataSchema.getGranularitySpec.isRollup
    )
    builder(inputFnFn(rollup, mkparser, timestampSpec), timestamperFn(timestampSpec))
      .curatorFactory(
        CuratorFrameworkFactory.builder()
          .connectString(config.propertiesBasedConfig.zookeeperConnect)
          .sessionTimeoutMs(config.propertiesBasedConfig.zookeeperTimeout.standardDuration.millis.toInt)
          .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
      )
      .discoveryPath(config.propertiesBasedConfig.discoPath)
      .clusteredBeamZkBasePath(config.propertiesBasedConfig.zookeeperPath)
      .location(DruidLocation(environment, fireDepartment.getDataSchema.getDataSource))
      .rollup(rollup)
      .timestampSpec(timestampSpec)
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = fireDepartment.getDataSchema.getGranularitySpec.getSegmentGranularity,
          windowPeriod = fireDepartment.getTuningConfig.getWindowPeriod,
          warmingPeriod = config.propertiesBasedConfig.taskWarmingPeriod
        )
      )
      .druidTuningMap(Option(config.specMap.getOrElse("tuningConfig", null)).map(dict(_)).getOrElse(Map.empty))
      .partitions(config.propertiesBasedConfig.taskPartitions)
      .replicants(config.propertiesBasedConfig.taskReplicants)
      .druidBeamConfig(config.propertiesBasedConfig.druidBeamConfig)
  }

  /**
    * Start a builder for a particular event type.
    *
    * @param timestamper timestamper for this event type
    * @tparam EventType the event type
    * @return a new builder
    */
  private[tranquility] def builder[InputType, EventType](
    inputFn: InputType => EventType,
    timestamper: Timestamper[EventType]
  ): Builder[InputType, EventType] =
  {
    new Builder[InputType, EventType](
      new BuilderConfig[InputType, EventType](
        _beamManipulateFn = Some(
          (beam: Beam[EventType]) => new TransformingBeam[InputType, EventType](beam, inputFn)
        ),
        _timestamper = Some(timestamper)
      )
    )
  }

  private def objectMapperAndContentTypeForFormat(formatString: String): (ObjectMapper, String) = {
    formatString match {
      case "json" => (DefaultScalaObjectMapper, MediaType.APPLICATION_JSON)
      case "smile" => (Jackson.newObjectMapper(new SmileFactory()), SmileMediaTypes.APPLICATION_JACKSON_SMILE)
      case _ =>
        throw new IllegalArgumentException("Unknown format: %s" format formatString)
    }
  }

  /**
    * Start a builder for a particular event type.
    *
    * @param timeFn time extraction function for this event type
    * @tparam EventType the event type
    * @return a new builder
    */
  def builder[EventType](timeFn: EventType => DateTime): Builder[EventType, EventType] = {
    builder(
      identity,
      new Timestamper[EventType]
      {
        override def timestamp(a: EventType): DateTime = timeFn(a)
      }
    )
  }

  /**
    * Start a builder for a particular event type.
    *
    * @param timestamper time extraction object for this event type
    * @tparam EventType the event type
    * @return a new builder
    */
  def builder[EventType]()(implicit timestamper: Timestamper[EventType]): Builder[EventType, EventType] = {
    new Builder[EventType, EventType](new BuilderConfig(_timestamper = Some(timestamper)))
  }

  /**
    * Return a new fireDepartment based on a config. It is important that this returns a *new* FireDepartment, as
    * this is used to pull out InputRowParsers, which are not thread safe and cannot be shared.
    */
  private[tranquility] def makeFireDepartment(
    config: DataSourceConfig[_]
  ): FireDepartment =
  {
    val specMap = Map("ioConfig" -> Dict("type" -> "realtime")) ++ config.specMap
    val fireDepartment = DruidGuicer.Default.objectMapper.convertValue(normalizeJava(specMap), classOf[FireDepartment])
    require(fireDepartment.getIOConfig.getFirehoseFactory == null, "Expected null 'firehose'")
    require(fireDepartment.getIOConfig.getFirehoseFactoryV2 == null, "Expected null 'firehoseV2'")
    require(fireDepartment.getIOConfig.getPlumberSchool == null, "Expected null 'plumber'")
    fireDepartment
  }

  class Builder[InputType, EventType] private[tranquility](
    private[tranquility] val config: BuilderConfig[InputType, EventType]
  )
  {
    /**
      * Provide a CuratorFramework instance that will be used for Tranquility's internal coordination. Either this
      * or "curatorFactory" is required. Whichever one is provided later will win.
      *
      * If you provide Curator this way (as opposed to "curatorFactory") then you should start and stop it yourself.
      * You can safely share it across multiple builders.
      *
      * If you do not provide your own [[Builder.finagleRegistry]], this will be used for service discovery as well.
      *
      * @param curator curator
      * @return new builder
      */
    def curator(curator: CuratorFramework): Builder[InputType, EventType] = {
      new Builder[InputType, EventType](config.copy(_curator = Some(curator), _curatorFactory = None))
    }

    /**
      * Provide a CuratorFrameworkFactory instance that will be used for Tranquility's internal coordination. Either
      * this or "curator" is required. Whichever one is provided later will win.
      *
      * If you provide Curator this way (as opposed to "curator") then the instance will be "owned" by this beam stack.
      * This means that when you close the beams or tranquilizers returned by this builder, the Curator instance will
      * be closed too.
      *
      * If you do not provide your own [[Builder.finagleRegistry]], this will be used for service discovery as well.
      *
      * @param curatorFactory curator factory
      * @return new builder
      */
    def curatorFactory(curatorFactory: CuratorFrameworkFactory.Builder): Builder[InputType, EventType] = {
      new Builder[InputType, EventType](config.copy(_curator = None, _curatorFactory = Some(curatorFactory)))
    }

    /**
      * Provide a znode used for Druid service discovery. Optional, defaults to "/druid/discovery".
      *
      * If you do not provide a [[Builder.finagleRegistry]], this will be used along with your provided
      * CuratorFramework to locate Druid services. If you do provide a FinagleRegistry, this option will not be used.
      *
      * @param path discovery znode
      * @return new builder
      */
    def discoveryPath(path: String) = new Builder[InputType, EventType](config.copy(_discoveryPath = Some(path)))

    /**
      * Provide tunings for coordination of Druid task creation. Optional, see
      * [[com.metamx.tranquility.beam.ClusteredBeamTuning$]] for defaults.
      *
      * These influence how and when Druid tasks are created.
      *
      * @param tuning tuning object
      * @return new builder
      */
    def tuning(tuning: ClusteredBeamTuning) = new Builder[InputType, EventType](config.copy(_tuning = Some(tuning)))

    /**
      * Set the number of Druid partitions. This is just a helper method that modifies the [[Builder.tuning]] object.
      *
      * @param n number of partitions
      * @return new builder
      */
    def partitions(n: Int) = {
      val newTuning = config._tuning.getOrElse(ClusteredBeamTuning()).copy(partitions = n)
      new Builder[InputType, EventType](config.copy(_tuning = Some(newTuning)))
    }

    /**
      * Set the number of Druid replicants. This is just a helper method that modifies the [[Builder.tuning]] object.
      *
      * @param n number of replicants
      * @return new builder
      */
    def replicants(n: Int) = {
      val newTuning = config._tuning.getOrElse(ClusteredBeamTuning()).copy(replicants = n)
      new Builder[InputType, EventType](config.copy(_tuning = Some(newTuning)))
    }

    /**
      * Provide tunings for the Druid realtime engine. Optional, see [[DruidTuning]] for defaults.
      *
      * These will be passed along to the Druid tasks.
      *
      * @param druidTuning tuning object
      * @return new builder
      */
    def druidTuning(druidTuning: DruidTuning) = {
      new Builder[InputType, EventType](config.copy(_druidTuningMap = Some(druidTuning.toMap)))
    }

    /**
      * Provide tunings for the Druid realtime engine. Optional, see [[DruidTuning]] for defaults.
      *
      * These will be passed along to the Druid tasks.
      *
      * @param druidTuningMap tuning object, as a map
      * @return new builder
      */
    def druidTuningMap(druidTuningMap: Dict) = {
      new Builder[InputType, EventType](config.copy(_druidTuningMap = Some(DruidTuning().toMap ++ druidTuningMap)))
    }

    /**
      * Provide the location of your Druid dataSource. Required.
      *
      * This will be used to determine the service name of your Druid overlord and tasks, and to choose which
      * dataSource to write to.
      *
      * @param location location object
      * @return new builder
      */
    def location(location: DruidLocation) = new Builder[InputType, EventType](config.copy(_location = Some(location)))

    /**
      * Provide rollup (dimensions, aggregators, query granularity). Required.
      *
      * @param rollup rollup object
      * @return new builder
      */
    def rollup(rollup: DruidRollup) = new Builder[InputType, EventType](config.copy(_rollup = Some(rollup)))

    /**
      * Provide a Druid timestampSpec. Optional, defaults to timestampColumn "timestamp" and timestampFormat "iso".
      *
      * Druid will use this to parse the timestamp of your serialized events.
      *
      * @param timestampSpec timestampSpec object
      * @return new builder
      */
    def timestampSpec(timestampSpec: TimestampSpec) = {
      new Builder[InputType, EventType](config.copy(_timestampSpec = Some(timestampSpec)))
    }

    /**
      * Provide the ZooKeeper znode that should be used for Tranquility's internal coordination. Optional, defaults
      * to "/tranquility/beams".
      *
      * @param path the path
      * @return new builder
      */
    def clusteredBeamZkBasePath(path: String) = {
      new Builder[InputType, EventType](config.copy(_clusteredBeamZkBasePath = Some(path)))
    }

    /**
      * Provide the identity of this Beam, which will be used for coordination. Optional, defaults to your overlord
      * service discovery key + "/" + your dataSource.
      *
      * All beams with the same identity coordinate with each other on Druid tasks.
      *
      * @param ident ident string
      * @return new builder
      */
    def clusteredBeamIdent(ident: String) = new Builder[InputType, EventType](
      config
        .copy(_clusteredBeamIdent = Some(ident))
    )

    /**
      * Provide tunings for communication with Druid tasks. Optional, see [[DruidBeamConfig]] for defaults.
      *
      * @param beamConfig beam config tunings
      * @return new builder
      */
    def druidBeamConfig(beamConfig: DruidBeamConfig) = {
      new Builder[InputType, EventType](config.copy(_druidBeamConfig = Some(beamConfig)))
    }

    /**
      * Provide an emitter that will be used to emit alerts. By default, alerts are emitted through a logger.
      *
      * @param emitter an emitter
      * @return new builder
      */
    def emitter(emitter: ServiceEmitter) = new Builder[InputType, EventType](config.copy(_emitter = Some(emitter)))

    /**
      * Provide a FinagleRegistry that will be used to generate clients for your Overlord. Optional, by default this
      * is built based on [[Builder.curator]] and [[Builder.discoveryPath]].
      *
      * @param registry a registry
      * @return new builder
      */
    def finagleRegistry(registry: FinagleRegistry) = {
      new Builder[InputType, EventType](config.copy(_finagleRegistry = Some(registry)))
    }

    /**
      * Provide a Timekeeper that will be used to determine what time it is for purposes of judging the windowPeriod.
      * Optional, by default this uses wall clock time. This is mostly useful for tests, as in real-world use it
      * is expected that you will be using wall clock time.
      *
      * @param timekeeper a timekeeper
      * @return new builder
      */
    def timekeeper(timekeeper: Timekeeper) = new Builder[InputType, EventType](
      config
        .copy(_timekeeper = Some(timekeeper))
    )

    /**
      * Provide a function that decorates each per-partition, per-interval beam. Optional, by default there is no
      * decoration. This is often used for gathering metrics.
      *
      * @param f function
      * @return new builder
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
      * @return new builder
      */
    def beamMergeFn(f: Seq[Beam[EventType]] => Beam[EventType]) = {
      if (config._partitioner.nonEmpty) {
        throw new IllegalStateException("Cannot set both 'beamMergeFn' and 'partitioner'")
      }
      new Builder[InputType, EventType](config.copy(_beamMergeFn = Some(f)))
    }

    /**
      * Provide a partitioner that determines how to route events when you have more than one Druid partition. By
      * default this uses time-and-dimensions based partitioning for simple Map types, and uses hashCode for custom
      * types. If you are using a custom type, you need to provide your own partitioner or beamMergeFn to get
      * optimal rollup. You cannot provide both a beamMergeFn and a partitioner.
      *
      * @param partitioner a partitioner
      * @return new builder
      */
    def partitioner(partitioner: Partitioner[EventType]) = {
      if (config._beamMergeFn.nonEmpty) {
        throw new IllegalStateException("Cannot set both 'beamMergeFn' and 'partitioner'")
      }
      new Builder[InputType, EventType](config.copy(_partitioner = Some(partitioner)))
    }

    /**
      * Provide extra information that will be emitted along with alerts. Optional, by default this is empty.
      *
      * @param d extra information
      * @return new builder
      */
    def alertMap(d: Dict) = new Builder[InputType, EventType](config.copy(_alertMap = Some(d)))

    @deprecated("use .objectWriter(...)", "0.2.21")
    def eventWriter(writer: ObjectWriter[EventType]) = {
      new Builder[InputType, EventType](config.copy(_objectWriter = Some(writer)))
    }

    /**
      * Provide a serializer for your event type. Optional, by default this uses a Jackson ObjectMapper.
      *
      * This method is designed use for Scala users.
      *
      * @param writer the serializer
      * @return new builder
      */
    def objectWriter(writer: ObjectWriter[EventType]) = {
      new Builder[InputType, EventType](config.copy(_objectWriter = Some(writer)))
    }

    /**
      * Provide a serializer for your event type. Optional, by default this uses a Jackson ObjectMapper.
      *
      * @param writer the serializer
      * @return new builder
      */
    def objectWriter(writer: JavaObjectWriter[EventType]) = {
      new Builder[InputType, EventType](config.copy(_objectWriter = Some(ObjectWriter.wrap(writer))))
    }

    def eventTimestamped(timeFn: EventType => DateTime) = {
      new Builder[InputType, EventType](
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
      *
      * @return a beam
      */
    def buildBeam(): Beam[InputType] = {
      val things = config.buildAll()
      things.overlordLocator.maybeAddResolvers(() => things.disco)
      things.taskLocator.maybeAddResolvers(
        () => things.disco,
        () => new DruidTaskResolver(things.indexService, things.timekeeper, things.druidBeamConfig.overlordPollPeriod)
      )
      val druidBeamMaker = new DruidBeamMaker[EventType](
        things.druidBeamConfig,
        things.location,
        things.tuning,
        things.druidTuningMap,
        things.rollup,
        things.timestampSpec,
        things.taskLocator,
        things.indexService,
        things.emitter,
        things.objectWriter,
        things.druidObjectMapper
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
      )(things.timestamper)
      if (things.curatorOwned) {
        things.curator.start()
      }
      things.beamManipulateFn(
        new Beam[EventType]
        {
          override def sendAll(messages: Seq[EventType]) = clusteredBeam.sendAll(messages)

          override def close() = clusteredBeam.close()
            .flatMap(_ => things.indexService.close())
            .map {
              _ => if (things.curatorOwned) {
                things.curator.close()
              }
            }

          override def toString = clusteredBeam.toString
        }
      )
    }

    /**
      * Build a Finagle Service using this DruidBeams builder. This simply wraps the beam.
      *
      * @return a service
      */
    @deprecated("use buildTranquilizer", "0.7.0")
    def buildService(): Service[Seq[InputType], Int] = {
      new BeamService(buildBeam())
    }

    /**
      * Build a Finagle Service using this DruidBeams builder, designed for Java users. This simply wraps the beam.
      *
      * @return a service
      */
    @deprecated("use buildTranquilizer", "0.7.0")
    def buildJavaService(): Service[ju.List[InputType], jl.Integer] = {
      val delegate = buildService()
      Service.mk((xs: ju.List[InputType]) => delegate(xs.asScala).map(Int.box))
    }

    /**
      * Build a Tranquilizer using this DruidBeams builder. This is a Finagle service, too, but unlike
      * [[Builder.buildService]] and [[Builder.buildJavaService]], it takes a single message at a time and does
      * batching for you.
      *
      * @return a service
      */
    def buildTranquilizer(): Tranquilizer[InputType] = {
      Tranquilizer.builder().build(buildBeam())
    }

    /**
      * Build a Tranquilizer using this DruidBeams builder. This is a Finagle service, too, but unlike
      * [[Builder.buildService]] and [[Builder.buildJavaService]], it takes a single message at a time and does
      * batching for you.
      *
      * @param builder a Tranquilizer builder with the desired configuration
      * @return a service
      */
    def buildTranquilizer(builder: Tranquilizer.Builder): Tranquilizer[InputType] = {
      builder.build(buildBeam())
    }
  }

  private[tranquility] case class BuilderConfig[InputType, EventType](
    _beamManipulateFn: Option[Beam[EventType] => Beam[InputType]] = None,
    _curator: Option[CuratorFramework] = None,
    _curatorFactory: Option[CuratorFrameworkFactory.Builder] = None,
    _discoveryPath: Option[String] = None,
    _tuning: Option[ClusteredBeamTuning] = None,
    _druidTuningMap: Option[Dict] = None,
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
      val beamManipulateFn        = _beamManipulateFn getOrElse {
        (beam: Beam[EventType]) => beam.asInstanceOf[Beam[InputType]]
      }
      val scalaObjectMapper       = DefaultScalaObjectMapper
      val druidObjectMapper       = DruidGuicer.Default.objectMapper
      val curator                 = _curator getOrElse {
        _curatorFactory.map(_.build()) getOrElse {
          throw new IllegalArgumentException("Missing 'curator' or 'curatorFactory'")
        }
      }
      val curatorOwned            = _curatorFactory.isDefined
      val tuning                  = _tuning getOrElse {
        ClusteredBeamTuning()
      }
      val druidTuningMap          = _druidTuningMap getOrElse {
        DruidTuning().toMap
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
      val clusteredBeamZkBasePath = _clusteredBeamZkBasePath getOrElse DefaultZookeeperPath
      val clusteredBeamIdent      = _clusteredBeamIdent getOrElse {
        "%s/%s" format(location.environment.indexServiceKey, location.dataSource)
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
      val timekeeper              = _timekeeper getOrElse new SystemTimekeeper
      val discoveryPath           = _discoveryPath getOrElse "/druid/discovery"
      val disco                   = new Disco(
        curator,
        new DiscoConfig
        {
          def discoAnnounce = None

          def discoPath = discoveryPath
        }
      )
      val finagleRegistry         = _finagleRegistry getOrElse {
        new FinagleRegistry(FinagleRegistryConfig(), Nil)
      }
      val overlordLocator         = OverlordLocator.create(
        druidBeamConfig.overlordLocator,
        finagleRegistry,
        location.environment
      )
      val indexService            = new IndexService(
        location.environment,
        druidBeamConfig,
        overlordLocator
      )
      val taskLocator             = TaskLocator.create(
        druidBeamConfig.taskLocator,
        finagleRegistry,
        location.environment
      )
      val beamDecorateFn          = _beamDecorateFn getOrElse {
        (interval: Interval, partition: Int) => (beam: Beam[EventType]) => beam
      }
      val alertMap                = _alertMap getOrElse Map.empty
      val objectWriter            = _objectWriter getOrElse {
        new DefaultJsonWriter(scalaObjectMapper)
      }
      val timestamper             = _timestamper getOrElse {
        throw new IllegalArgumentException("WTF?! Should have had a Timestamperable event...")
      }
      val beamMergeFn             = _beamMergeFn getOrElse {
        val partitioner = _partitioner getOrElse {
          MapPartitioner.create(
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
