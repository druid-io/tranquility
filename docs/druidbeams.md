# DruidBeams

All Tranquility APIs need a Beam to get started. The simplest way to create a Beam is to use the DruidBeams builder.
The minimum requirements are:

  - Event type, which could be something simple like `Map<String, Object>` or could be any type you choose. Unless
  you provide a custom serializer, this event type must be serializable by Jackson.
  - Timestamper, which tells Tranquility how to extract timestamps from your event type.
  - Druid cluster location.
  - Druid indexing configuration, including dimensions, aggregators, a queryGranularity, and so on.
  - CuratorFramework instance pointing to a ZooKeeper cluster used for coordination and Druid service discovery.

Many other options are configurable as well.

## Configuration reference

Full configuration reference is available at: http://static.druid.io/tranquility/api/latest/#com.metamx.tranquility.druid.DruidBeams$

## Example

Here's an example of using the DruidBeams builder to create a simple Beam:

```java
final Beam<Map<String, Object>> druidService = DruidBeams
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
    .buildBeam();
```

For more examples, see the [Core API documentation](core.md).
