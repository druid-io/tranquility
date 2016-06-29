# Spark

If you're using Spark to process your event stream, you can use Tranquility's BeamRDD adapter to send data to
Druid. The BeamRDD class provides any RDD the ability to write to Druid using the propagate() function.
The BeamRDD requires a BeamFactory to propagate events. You will be overriding the makeBeam() function and within that
using the DruidBeams builder's "buildBeam()" to build the beam. See the [Configuration documentation](configuration.md)
for details.

It is recommended that you implement makeBeam using a singleton so the Beam can be reused.

For example:

```scala

class SimpleEventBeamFactory extends BeamFactory[SimpleEvent]
{
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[SimpleEvent] = SimpleEventBeamFactory.BeamInstance
}

object SimpleEventBeamFactory
{
  val BeamInstance: Beam[SimpleEvent] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "localhost:2181",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo"
    val dimensions = IndexedSeq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))

    // Expects simpleEvent.timestamp to return a Joda DateTime object.
    DruidBeams
      .builder((simpleEvent: SimpleEvent) => simpleEvent.timestamp)
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation(indexService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.MINUTE))
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

// Add this import to your Spark job to be able to propagate events from any RDD to Druid
import com.metamx.tranquility.spark.BeamRDD._

// Now given a Spark DStream, you can send events to Druid.
dstream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory))
```
