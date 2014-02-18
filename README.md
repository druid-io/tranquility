## Tranquility

Tranquility helps you send event streams to Druid, the raddest data store ever (http://druid.io/), in real-time. It
handles partitioning, replication, service discovery, and schema rollover for you, seamlessly and without downtime.
Tranquility is written in Scala, and bundles idiomatic Java and Scala APIs that work nicely with Finagle, Storm, and
Trident.

This project is a friend of Druid. For discussion, feel free to use the normal Druid channels: http://druid.io/community.html

## Finagle

For general purposes, you'll likely end up using the Finagle API. You can set up and use a Finagle Service like this:

```scala
val dataSource = "foo"
val dimensions = Seq("bar")
val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))
val druidService = DruidBeams
  .builder[Map[String, Any]](eventMap => new DateTime(eventMap("timestamp")))
  .curator(curator)
  .discoveryPath("/test/discovery")
  .location(DruidLocation(new DruidEnvironment("druid:overlord", "druid:firehose:%s"), dataSource))
  .rollup(DruidRollup(dimensions, aggregators, QueryGranularity.MINUTE))
  .tuning(ClusteredBeamTuning(Granularity.HOUR, 10.minutes, 1, 1))
  .buildService()

// Send events to Druid:
val numSent: Future[Int] = druidService(listOfEvents)
```

Or in Java:

```java
final String dataSource = "hey";
final List<String> dimensions = ImmutableList.of("column");
final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
    new CountAggregatorFactory(
        "cnt"
    )
);

final Service<List<Map<String, Object>>, Integer> druidService = DruidBeams
    .builder(
        new Timestamper<Map<String, Object>>()
        {
          @Override
          public DateTime timestamp(Map<String, Object> theMap)
          {
            return new DateTime(theMap.get("timestamp"));
          }
        }
    )
    .curator(curator)
    .discoveryPath("/test/discovery")
    .location(
        new DruidLocation(
            new DruidEnvironment(
                "druid:overlord",
                "druid:firehose:%s"
            ), dataSource
        )
    )
    .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularity.MINUTE))
    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT10M"), 1, 1))
    .buildJavaService();

// Send events to Druid:
final Future<Integer> numSent = druidService.apply(listOfEvents);
```

## Storm

If you're using Storm to generate your event stream, you can use Tranquility's builtin Bolt adapter. This Bolt expects
to receive tuples in which the zeroth element is your event type (in this case, Scala Maps). It does not emit any
tuples of its own.

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
    DruidBeams
      .builder[Map[String, Any]](eventMap => new DateTime(eventMap("timestamp")))
      .curator(curator)
      .discoveryPath("/test/discovery")
      .location(DruidLocation(new DruidEnvironment("druid:overlord", "druid:firehose:%s"), dataSource))
      .rollup(DruidRollup(dimensions, aggregators, QueryGranularity.MINUTE))
      .tuning(ClusteredBeamTuning(Granularity.HOUR, 10.minutes, 1, 1))
      .buildBeam()
  }
}

// Add this bolt to your topology:
val bolt = new BeamBolt(new MyBeamFactory)
```

## Trident

If you're using Trident on top of Storm, you can use Trident's partitionPersist in concert with Tranquility's
TridentBeamStateFactory (which takes a BeamFactory, like the Storm Bolt) and TridentBeamStateUpdater.

## Druid Setup

Tranquility works with the Druid indexing service (http://druid.io/docs/latest/Indexing-Service.html). To get started, you'll need an Overlord, enough Middle Managers for your realtime workload, and enough Historical nodes to receive handoffs. You don't need any Realtime nodes, since Tranquility uses the indexing service for all of its ingestion needs.

Tranquility periodically submits new tasks to the indexing service to provide for log rotation and to support zero-downtime configuration changes. These new tasks are typically submitted before the old ones exit, so to allow for smooth transitions, you'll need enough indexing service worker capacity to run two sets of overlapping tasks (that's 2 * #partitions * #replicants). The number of partitions and replicants both default to 1 (single partition, single copy) and can be tuned using a ClusteredBeamTuning object.
