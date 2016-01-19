## Tranquility

> &#8220;Stay close, my friends, and I will heal your wounds.&#8221;<br />
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&mdash;Mythen of the Wild

Tranquility helps you send event streams to Druid, the raddest data store ever (http://druid.io/), in real-time. It
handles partitioning, replication, service discovery, and schema rollover for you, seamlessly and without downtime.
Tranquility is written in Scala, and bundles idiomatic Java and Scala APIs that work nicely with Finagle, Samza, Spark,
Storm, and Trident.

This project is a friend of Druid. For discussion, feel free to use the normal Druid channels: http://druid.io/community/

### Documentation

General:

- [Overview](docs/overview.md) - Introduction to Tranquility concepts, including details about how it creates and
  manages Druid tasks.
- [DruidBeams](docs/druidbeams.md) - The first step to using Tranquility is to configure it appropriately for your
  Druid dataSource. For most modules (including core, storm, spark, and samza) this is generally done through the
  DruidBeams builder object.
- [Troubleshooting](docs/trouble.md) - Solutions to common problems.

Modules:

- [Core](docs/core.md) - The most basic data-sending API. You will likely use this one unless you are using
  one of the higher-level modules.
- [Server](docs/server.md) - HTTP server that allows you to use Tranquility without developing a JVM app.
- [Samza](docs/samza.md) - Tranquility includes a Samza SystemProducer.
- [Spark](docs/spark.md) - Tranquility works with RDDs and DStreams.
- [Storm](docs/storm.md) - Tranquility includes a Storm Bolt and a Trident State.
- [Kafka](docs/kafka.md) - Application to push messages from Kafka into Druid through Tranquility.
- [Flink](docs/flink.md) - Tranquility includes a Flink Sink.

### Getting Tranquility with Maven

Tranquility [Core](docs/core.md), [Samza](docs/samza.md), [Spark](docs/spark.md), and [Storm](docs/storm.md) are
meant to be included in an application that you write. Those modules are hosted on Maven Central to make them
easy to include. The current stable versions are:

```xml
<dependency>
  <groupId>io.druid</groupId>
  <artifactId>tranquility-core_2.11</artifactId>
  <version>0.7.2</version>
</dependency>
<dependency>
  <groupId>io.druid</groupId>
  <artifactId>tranquility-samza_2.10</artifactId>
  <version>0.7.2</version>
</dependency>
<dependency>
  <groupId>io.druid</groupId>
  <artifactId>tranquility-spark_2.11</artifactId>
  <version>0.7.2</version>
</dependency>
<dependency>
  <groupId>io.druid</groupId>
  <artifactId>tranquility-storm_2.11</artifactId>
  <version>0.7.2</version>
</dependency>
```

You only need to include the modules you are actually using.

All Tranquility modules are built for both Scala 2.10 and 2.11, except for the Samza module, which is only built for
Scala 2.10. If you're using Scala for your own code, you should choose the Tranquility build that matches your version
of Scala. Otherwise, Scala 2.11 is recommended.

This version is built to work with Druid 0.7.x and 0.8.x. If you are using Druid 0.6.x, you may want to use Tranquility
v0.3.2, which is the most recent version built for use with Druid 0.6.x.

Tranquility is built with [SBT](http://www.scala-sbt.org/). If you want to build the jars yourself, you can
run `sbt +package`.

### Downloadable Distribution

The Tranquility downloadable distribution includes the [Server](docs/server.md) and [Kafka](docs/kafka.md) programs,
which are standalone programs that can be used without writing any code. You can download the distribution and
run them directly. The distribution also includes the [Core API](docs/core.md) artifacts, if you prefer to download
them rather than get them through Maven.

The current distribution is:
[tranquility-distribution-0.7.2](http://static.druid.io/tranquility/releases/tranquility-distribution-0.7.2.tgz).

To use it, first download it and then unpack it into your directory of choice by running
`tar -xzf tranquility-distribution-0.7.2.tgz`.

### How to Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md).
