# Spark

If you're using Spark to process your event stream, you can use Tranquility's BeamRDD adapter to send data to
Druid. The BeamRDD class provides any RDD the ability to write to Druid using the propagate() function.
The BeamRDD requires a BeamFactory to propagate events. You will be overriding the makeBeam() function and within that
using the DruidBeams builder's "buildBeam()" to build the beam. See the [DruidBeams documentation](druidbeams.md)
for details about creating beams.

It is recommended that you implement makeBeam as a lazy val so the Beam can be reused.

For example:

```scala

class SimpleEventBeamFactory extends BeamFactory[SimpleEvent] {

lazy val makeBeam : Beam[SimpleEvent] = {

    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "localhost:2181",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val indexService = "overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val firehosePattern = "druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo"
    val dimensions = IndexedSeq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))

    DruidBeams
      .builder((eventMap: SimpleEvent) => new DateTime())
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

// Add this import to your Spark job to be able to propagate events from any RDD to Druid
import com.metamx.tranquility.spark.BeamRDD._

//now given a spark dstream, you could propagate events
dstream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory))
