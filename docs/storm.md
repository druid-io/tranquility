# Storm

If you're using Storm to process your event stream, you can use Tranquility's builtin Bolt adapter to send data to
Druid. This Bolt expects to receive tuples in which the zeroth element is your event type (in this case, Scala Maps).
It does not emit any tuples of its own.

It must be supplied with a BeamFactory. You can implement one of these using the DruidBeams builder's "buildBeam()"
method. See the [DruidBeams documentation](druidbeams.md) for details about creating beams.

For example:

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
    val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path.
    val dataSource = "foo"
    val dimensions = Seq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))

    DruidBeams
      .builder((eventMap: Map[String, Any]) => new DateTime(eventMap("timestamp")))
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation(indexService, dataSource))
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

## Storm Setup

Tranquility depends on a newer version of zookeeper than Storm is built with, at least through Storm 0.9.1. This should
be worked out once [STORM-70](https://issues.apache.org/jira/browse/STORM-70) is in a release, but for the time being,
Tranquility deployments will work better on a patched Storm. Our fork is here: https://github.com/metamx/incubator-storm/tree/v0.9.1-incubating-mmx.
We have also published artifacts in the the metamx maven repository at: https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local/org/apache/storm/storm-core/.
