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

## Examples

- [Java example](https://github.com/druid-io/tranquility/blob/master/core/src/test/java/com/metamx/tranquility/example/JavaExample.java)
- [Scala example](https://github.com/druid-io/tranquility/blob/master/core/src/test/scala/com/metamx/tranquility/example/ScalaExample.scala)

These examples use DruidBeams with the [Core API](core.md). However, the DruidBeams creation process is the same for
all APIs, not just the Core API.
