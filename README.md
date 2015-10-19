## Tranquility

Tranquility helps you send event streams to Druid, the raddest data store ever (http://druid.io/), in real-time. It
handles partitioning, replication, service discovery, and schema rollover for you, seamlessly and without downtime.
Tranquility is written in Scala, and bundles idiomatic Java and Scala APIs that work nicely with Finagle, Samza, Storm,
and Trident.

This project is a friend of Druid. For discussion, feel free to use the normal Druid channels: http://druid.io/community/

## Direct API

If you want to write a program that sends data to Druid, you'll likely end up using the direct Finagle-based API. (The
other alternatives are the Samza and Storm APIs, described in the following sections.)

You can set up and use a Finagle Service like this:

```java
final String indexService = "overlord"; // Your overlord's service name.
final String firehosePattern = "druid:firehose:%s"; // Make up a service pattern, include %s somewhere in it.
final String discoveryPath = "/druid/discovery"; // Your overlord's druid.discovery.curator.path
final String dataSource = "foo";
final List<String> dimensions = ImmutableList.of("bar", "qux");
final List<AggregatorFactory> aggregators = ImmutableList.of(
    new CountAggregatorFactory("cnt"),
    new LongSumAggregatorFactory("baz", "baz")
);

// Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>()
{
    @Override
    public DateTime timestamp(Map<String, Object> theMap)
    {
        return new DateTime(theMap.get("timestamp"));
    }
};

// Tranquility uses ZooKeeper (through Curator) for coordination.
final CuratorFramework curator = CuratorFrameworkFactory
    .builder()
    .connectString("zk.example.com:2181")
    .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
    .build();
curator.start();

// The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
// Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto");

// Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
// done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
// In this case, we won't provide one, so we're just using Jackson.
final Service<List<Map<String, Object>>, Integer> druidService = DruidBeams
    .builder(timestamper)
    .curator(curator)
    .discoveryPath(discoveryPath)
    .location(
        DruidLocation.create(
            indexService,
            firehosePattern,
            dataSource
        )
    )
    .timestampSpec(timestampSpec)
    .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularity.MINUTE))
    .tuning(
      ClusteredBeamTuning
        .builder()
        .segmentGranularity(Granularity.HOUR)
        .windowPeriod(new Period("PT10M"))
        .partitions(1)
        .replicants(1)
        .build()
    )
    .buildJavaService();

// Send events to Druid:
final Future<Integer> numSentFuture = druidService.apply(listOfEvents);

// Wait for confirmation:
final Integer numSent = Await.result(numSentFuture);

// Close lifecycled objects:
Await.result(druidService.close());
curator.close();
```

Or in Scala:

```scala
val indexService = "overlord" // Your overlord's druid.service, with slashes replaced by colons.
val firehosePattern = "druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path.
val dataSource = "foo"
val dimensions = Seq("bar", "qux")
val aggregators = Seq(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("baz", "baz"))

// Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
val timestamper = (eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp"))

// Tranquility needs to be able to serialize your object type. By default this is done with Jackson. If you want to
// provide an alternate serializer, you can provide your own via ```.objectWriter(...)```. In this case, we won't
// provide one, so we're just using Jackson:
val druidService = DruidBeams
  .builder(timestamper)
  .curator(curator)
  .discoveryPath(discoveryPath)
  .location(DruidLocation(indexService, firehosePattern, dataSource))
  .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE))
  .tuning(
    ClusteredBeamTuning(
      segmentGranularity = Granularity.HOUR,
      windowPeriod = new Period("PT10M"),
      partitions = 1,
      replicants = 1
    )
  )
  .buildService()

// Send events to Druid:
val numSentFuture: Future[Int] = druidService(listOfEvents)

// Wait for confirmation:
val numSent = Await.result(numSentFuture)
```

## Samza

If you're using Samza to process your event stream, you have two options for sending a stream to Druid:

- Use Samza's builtin Kafka support to write messages to a Kafka topic, then use Druid's
[Kafka 0.8 support](http://druid.io/docs/latest/Kafka-Eight.html) to read from the same Kafka topic. This method does
not involve Tranquility at all.
- Use Tranquility's builtin Samza SystemFactory to send data to Druid over HTTP.

Since this is the Tranquility documentation, we'll talk about the second method. You can set up a Samza job with the
following properties:

```
systems.druid.samza.factory: com.metamx.tranquility.samza.BeamSystemFactory
systems.druid.beam.factory: com.example.MyBeamFactory
systems.druid.beam.batchSize: 2000 # Optional; we'll send batches of this size to Druid
systems.druid.beam.maxPendingBatches: 5 # Optional; maximum number of in-flight batches before the producer blocks
```

To make this work, you need to implement com.example.MyBeamFactory. For example:

```java
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.samza.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

public class MyBeamFactory implements BeamFactory
{

  @Override
  public Beam<Object> makeBeam(SystemStream stream, Config config)
  {
    final String zkConnect = "localhost";
    final String dataSource = stream.getStream();

    final List<String> dimensions = ImmutableList.of("column");
    final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
        new CountAggregatorFactory("cnt")
    );

    // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
    // Objects, so you'll generally have to cast them here.
    final Timestamper<Object> timestamper = new Timestamper<Object>()
    {
      @Override
      public DateTime timestamp(Object obj)
      {
        final Map<String, Object> theMap = (Map<String, Object>) obj;
        return new DateTime(theMap.get("timestamp"));
      }
    };

    final CuratorFramework curator = CuratorFrameworkFactory.builder()
                                                            .connectString(zkConnect)
                                                            .retryPolicy(new ExponentialBackoffRetry(500, 15, 10000))
                                                            .build();
    curator.start();

    return DruidBeams
        .builder(timestamper)
        .curator(curator)
        .discoveryPath("/druid/discovery")
        .location(DruidLocation.create("overlord", "druid:firehose:%s", dataSource))
        .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularity.MINUTE))
        .tuning(
            ClusteredBeamTuning.builder()
                               .segmentGranularity(Granularity.HOUR)
                               .windowPeriod(new Period("PT10M"))
                               .build()
        )
        .buildBeam();
  }
}
```

## Storm

If you're using Storm to process your event stream, you can use Tranquility's builtin Bolt adapter to send data to
Druid. This Bolt expects to receive tuples in which the zeroth element is your event type (in this case, Scala Maps).
It does not emit any tuples of its own.

It must be supplied with a BeamFactory. You can implement one of these using the DruidBeams builder's "buildBeam()"
method. For example:

```scala
class MyBeamFactory extends BeamFactory[Map[String, Any]]
{
  def makeBeam(conf: java.util.Map[_, _], metrics: IMetricsContext) = {
    // This means you'll need a "tranquility.zk.connect" property in your Storm topology.
    val curator = CuratorFrameworkFactory.newClient(
      conf.get("tranquility.zk.connect").asInstanceOf[String],
      new BoundedExponentialBackoffRetry(100, 1000, 5)
    )
    curator.start()

    val indexService = "overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val firehosePattern = "druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
    val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path.
    val dataSource = "foo"
    val dimensions = Seq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))

    DruidBeams
      .builder((eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp")))
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation(indexService, firehosePattern, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      )
      .buildBeam()
  }
}

// Add this bolt to your topology:
val bolt = new BeamBolt(new MyBeamFactory)
```

## Trident

If you're using Trident on top of Storm, you can use Trident's partitionPersist in concert with Tranquility's
TridentBeamStateFactory (which takes a BeamFactory, like the Storm Bolt) and TridentBeamStateUpdater.

## JARs

Tranquility artifacts are hosted on Maven Central. The current stable version is:

```xml
<dependency>
  <groupId>io.druid</groupId>
  <artifactId>tranquility_2.10</artifactId>
  <version>0.5.0</version>
</dependency>
```

This version is built to work with Druid 0.7.x. If you are using Druid 0.6.x, you may want to use tranquility v0.3.2, which
is the most recent version built for use with Druid 0.6.x.

Tranquility is built with [SBT](http://www.scala-sbt.org/). If you want to build the jars yourself, you can
run ```sbt package```.

## Storm Setup

Tranquility depends on a newer version of zookeeper than Storm is built with, at least through Storm 0.9.1. This should
be worked out once [STORM-70](https://issues.apache.org/jira/browse/STORM-70) is in a release, but for the time being,
Tranquility deployments will work better on a patched Storm. Our fork is here: https://github.com/metamx/incubator-storm/tree/v0.9.1-incubating-mmx.
We have also published artifacts in the the metamx maven repository at: https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local/org/apache/storm/storm-core/.

## Druid Setup

Tranquility works with the Druid indexing service (http://druid.io/docs/latest/Indexing-Service.html). To get started,
you'll need an Overlord, enough Middle Managers for your realtime workload, and enough Historical nodes to receive
handoffs. You don't need any Realtime nodes, since Tranquility uses the indexing service for all of its ingestion needs.

Tranquility periodically submits new tasks to the indexing service to provide for log rotation and to support
zero-downtime configuration changes. These new tasks are typically submitted before the old ones exit, so to allow for
smooth transitions, you'll need enough indexing service worker capacity to run two sets of overlapping tasks (that's
2 * #partitions * #replicants). The number of partitions and replicants both default to 1 (single partition, single
copy) and can be tuned using a ClusteredBeamTuning object.

## Guarantees

Tranquility operates under a best-effort design. It tries reasonably hard to preserve your data, by allowing you to
set up replicas and by retrying failed pushes for a period of time, but it does not guarantee that your events will be
processed exactly once. In some conditions, it can drop or duplicate events:

- Events with timestamps outside your configured windowPeriod will be dropped.
- If you suffer more Druid Middle Managers failures than your configured replicas count, some partially indexed data
may be lost.
- If there is a persistent issue that prevents communication with the Druid indexing service, and retry policies are
exhausted during that period, or the period lasts longer than your windowPeriod, some events will be dropped.
- If there is an issue that prevents Tranquility from receiving an acknowledgement from the indexing service, it will
retry the batch, which can lead to duplicated events.
- If you are using Tranquility inside Storm or Samza, various parts of the both architectures have an at-least-once
design and can lead to duplicated events.

Our approach at Metamarkets is to send all of our data through Tranquility in real-time, but to also mitigate these
risks by storing a copy in S3 and following up with a nightly Hadoop batch indexing job to re-ingest the data. This
allow us to guarantee that in the end, every event is represented exactly once in Druid. The setup mitigates other risks
as well, including the fact that data can come in later than expected (if a previous part of the pipeline is delayed
by more than the windowPeriod) or may need to be revised after initial ingestion.

## Troubleshooting

### I'm getting strange Jackson or Curator exceptions.

Most of Tranquility uses com.fasterxml.jackson 2.4.x, but Curator is still built against the older org.codehaus.jackson.
It requires at least 1.9.x, and people have reported strange errors when using older versions of Jackson (usually
1.8.x). Tranquility tries to pull in Jackson 1.9.x, but this may be overridden in your higher-level project file. If
you see any strange Jackson or Curator errors, try confirming that you are using the right version of Jackson. These
errors might include the following:

- java.io.NotSerializableException: org.apache.curator.x.discovery.ServiceInstance
- org.codehaus.jackson.map.exc.UnrecognizedPropertyException: Unrecognized field "name" (Class org.apache.curator.x.discovery.ServiceInstance), not marked as ignorable

To force a particular Jackson version, you can use something like this in your POM:

```xml
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-jaxrs</artifactId>
    <version>1.9.13</version>
</dependency>
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-xc</artifactId>
    <version>1.9.13</version>
</dependency>
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-core-asl</artifactId>
    <version>1.9.13</version>
</dependency>
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-mapper-asl</artifactId>
    <version>1.9.13</version>
</dependency>
```
