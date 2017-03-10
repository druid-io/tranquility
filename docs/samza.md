# Samza

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

To make this work, you need to implement com.example.MyBeamFactory. It should return a `Beam<Object>`. See the
[Configuration documentation](configuration.md) for details.

For example:

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
    final Boolean isRollup = true;
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
        .location(DruidLocation.create("druid/overlord", "druid:firehose:%s", dataSource))
        .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularities.MINUTE, isRollup))
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
