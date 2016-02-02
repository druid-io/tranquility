# Kafka

Tranquility Kafka is an application which simplifies the ingestion of data from Kafka. It is scalable and highly
available through the use of Kafka partitions and consumer groups, and can be configured to push data from multiple
Kafka topics into multiple Druid dataSources.

## Setup

### Getting Tranquility Kafka

Tranquility Kafka is included in the [downloadable distribution](../README.md#downloadable-distribution).

### Configuration

Tranquility Kafka uses a standard [Tranquility configuration file](configuration.md) with some extra properties.
There's an example file at `conf/server.json.example` of the tarball distribution. You can start off your installation
by copying that file to `conf/server.json` and customizing it for your own setup.

These Kafka-specific properties, if used, must be specified at the global level:

|Property|Description|Default|
|--------|-----------|-------|
|`kafka.zookeeper.connect`|ZooKeeper connect string for Kafka.|none; must be provided|
|`kafka.group.id`|Group ID for Kafka consumers. **Note:** This must be the same for all instances to avoid data duplication!|tranquility-kafka|
|`consumer.numThreads`|The number of threads that will be made available to the Kafka consumer for fetching messages.|{numProcessors} - 1|
|`commit.periodMillis`|The frequency with which consumer offsets will be committed to ZooKeeper to track processed messages.|15000|
|`kafka.*`|Any properties that begin with *kafka.* will be passed to the underlying Kafka consumer with the *kafka.* prefix removed. For example, if you set `kafka.consumer.id=myConsumer`, the Kafka consumer will be passed the property `consumer.id=myConsumer`. Note that Tranquility Kafka requires `auto.commit.enable` to be *false* and any attempt to set this property will be overridden. |none|

These Kafka-specific properties, if used, may be specified either at the global level or at the dataSource level:

|Property|Description|Default|
|--------|-----------|-------|
|`topicPattern`|A regular expression used to match Kafka topics to dataSource configurations. See "Matching Topics to Data Sources" for details.|{match nothing}, must be provided|
|`topicPattern.priority`|If multiple topicPatterns match the same topic name, the highest priority dataSource configuration will be used. A higher number indicates a higher priority. See "Matching Topics to Data Sources" for details.|1|
|`useTopicAsDataSource`|Use the Kafka topic as the dataSource name instead of the one provided in the configuration file. Useful when combined with a topicPattern that matches more than one Kafka topic. See "Matching Topics to Data Sources" for details.|false|
|`reportDropsAsExceptions`|Whether or not dropped messages will cause an exception and terminate the application.|false|

### Running

If you've saved your configuration into `conf/tranquility-kafka.json`, run the application with:

```bash
bin/tranquility kafka -configFile conf/tranquility-kafka.json
```

## Matching Topics to Data Sources

Tranquility Kafka supports matching multiple Kafka topics to Druid dataSources through the use of the regex-based
`topicPattern` property. A topic can be mapped to at most one dataSource at a time; if there are multiple topic patterns
that match a given Kafka topic, the one with the highest-valued `topicPattern.priority` will be used.

This can be combined with the `useTopicAsDataSource` property to map multiple topics to multiple similar dataSources
without having to add repeated configurations for each dataSource. When `useTopicAsDataSource` is set to `true`, the
dataSource name for this entry will be ignored and the Kafka topic will be used instead. Since the `topicPattern`
matches using regular expressions, this can be used to watch for Kafka topics that did not exist when the application
was started and dynamically create new Druid dataSources with the topic as its name.

As an example, consider the following configuration (unrelated properties removed for clarity):

```
dataSources:
  wikipedia:
    spec:
      dataSchema:
        dataSource: wikipedia
    properties:
      topicPattern: wikipedia
      topicPattern.priority: 2

  wikipedia-lang:
    spec:
      dataSchema:
        dataSource: wikipedia-lang
    properties:
      topicPattern: wikipedia.*
      useTopicAsDataSource: true
```

Given the list of Kafka topics *[wikipedia, wikipedia-fr, wikipedia-es]*:

  - The topic *wikipedia* will be matched to the `wikipedia` configuration, even though it also matches `wikipedia-lang`
  (order in the configuration file does not matter) since `wikipedia` has a higher `topicPattern.priority` than `wikipedia-lang`
  (which has the default priority of 1). This will be mapped to a dataSource named **wikipedia**.
  - The topics *wikipedia-fr* and *wikipedia-es* will both be matched to `wikipedia-lang`. Since `useTopicAsDataSource`
  is set to true, they will be mapped to dataSources named **wikipedia-fr** and **wikipedia-es** respectively and will
  not be both mapped to a dataSource named **wikipedia-lang** (which is what would happen if `useTopicAsDataSource` was
  false).
  - In the future, if we were interested in *wikipedia-de* and created a new Kafka topic for it, this new topic would be
   automatically discovered and matched to the `wikipedia-lang` configuration which will result in a new dataSource
   **wikipedia-de** being created.


## Deployment

Multiple instances of Tranquility Kafka may be deployed to provide scalability (through each instance owning a subset
of the Kafka partitions) and redundancy (through all instances sharing the same Kafka group ID). If the configuration
changes, instances can be brought down and updated one at a time to update the cluster without any downtime.

If the update was to add a new configuration, the first updated instance will begin receiving all the messages from the
newly-mapped Kafka topics until subsequent instances are updated which will trigger a load rebalance. If the update was
to change an existing Druid ingestion spec, the old specification will remain in effect for the remainder of the
segmentGranularity period and the new ingestion spec will be applied to the indexing tasks handling the next segment.

Be aware that changing the name of the dataSource a Kafka topic is mapped to via rolling update will cause some of the
topic's partitions to be written to the old dataSource and some to be written to the new dataSource during the
transition period. This may or may not be acceptable depending on your use case. For a clean transition, it is suggested
that you stop all old instances of Tranquility Kafka before starting the set of new ones, allowing Kafka to buffer
events that were received during the update period.
