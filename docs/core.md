# Core

If you want to write a program that sends data to Druid, you'll likely end up using the direct Finagle-based API.

## DruidBeams

To start using the Core API, the first thing you need to do is build up a DruidBeams stack. See the
[DruidBeams documentation](druidbeams.md) for details.

## Java example

You can set up and use a Finagle Service like this:

```java
final String indexService = "druid/overlord"; // Your overlord's service name.
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
    .location(DruidLocation.create(indexService, dataSource))
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

// Build a sample event to send; make sure we use a current date
Map<String, Object> obj = ImmutableMap.of("timestamp", new DateTime().toString(), "bar", "barVal", "baz", 3);

// Send events to Druid:
final Future<Integer> numSentFuture = druidService.apply(ImmutableList.of(obj));

// Wait for confirmation:
final Integer numSent = Await.result(numSentFuture);

// Close lifecycled objects:
Await.result(druidService.close());
curator.close();
```

## Scala example

```scala
val indexService = "overlord" // Your overlord's druid.service, with slashes replaced by colons.
val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path.
val dataSource = "foo"
val dimensions = Seq("bar", "qux")
val aggregators = Seq(new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("baz", "baz"))

// Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
val timestamper = (eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp"))

// Tranquility uses ZooKeeper (through Curator) for coordination.
val curator = CuratorFrameworkFactory
  .builder()
  .connectString("zk.example.com:2181")
  .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
  .build()
curator.start()

// Tranquility needs to be able to serialize your object type. By default this is done with Jackson. If you want to
// provide an alternate serializer, you can provide your own via ```.objectWriter(...)```. In this case, we won't
// provide one, so we're just using Jackson:
val druidService = DruidBeams
  .builder(timestamper)
  .curator(curator)
  .discoveryPath(discoveryPath)
  .location(DruidLocation.create(indexService, dataSource))
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

// Build a sample event to send; make sure we use a current date
val obj = Map("timestamp" -> new DateTime().toString, "bar" -> "barVal", "baz" -> 3)

// Send events to Druid:
val numSentFuture: Future[Int] = druidService(Seq(obj))

// Wait for confirmation:
val numSent = Await.result(numSentFuture)

// Close lifecycled objects:
Await.result(druidService.close())
curator.close()
```
