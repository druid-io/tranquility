# Troubleshooting

## No data is showing up in Druid, even though events seem to be successfully sent.

The most common reason for this is that your events are outside the configured windowPeriod. By default, this is ten
minutes, which means that any events with a timestamp older than ten minutes in the past will be dropped. Some things
to check are:

  1. Make sure you are sending events within the windowPeriod. See the
  [overview](overview.md#segment-granularity-and-window-period) for more information about how this works.
  2. Make sure you have set up an appropriate Timestamper and TimestampSpec, so Tranquility and Druid both know
  what timestamp your events have.

## My tasks are never exiting.

The most common reason for this is that handoff is not occurring- your historical nodes are not loading the segments
created by your realtime tasks (see the [overview](overview.md) for more information about how this works).

Ensure that your coordinator and historical nodes are running, and that your historical nodes have sufficient capacity
to load new segments. You'll see warnings or errors in the coordinator logs if they do not.

Another possibility is that your windowPeriod is excessively long. Keep in mind that tasks cannot hand off and exit
until the segmentGranularity interval is over and the windowPeriod has elapsed.

## I'm getting strange Jackson or Curator exceptions.

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
