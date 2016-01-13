# Server

Tranquility Server lets you use Tranquility to send data to Druid without developing a JVM app. You only need an
HTTP client. Tranquility Server is easy to set up: a single server process can handle multiple dataSources, and you can
scale out by simply starting more server processes and putting them behind a load balancer or DNS round-robin.

## HTTP API

You can send data through HTTP POST. You can send as little or as much data at once as you like. The form of the
request should be:

- URL `/v1/post` or `/v1/post/DATASOURCE`.
- Content-Type `application/json` or `application/x-jackson-smile`.
- JSON or Smile data matching the Content-Type you provided. If JSON, you can provide either an array of JSON objects
or a sequence of newline-delimited JSON objects. If Smile, you should provide an array of objects.
- If using the URL `/v1/post`, your objects must have a field named "dataSource" or "feed" containing the destination
dataSource for that object.

The response will be:

```json
{
  "result": {
    "received": 10,
    "sent": 10
  }
}
```

Where *received* is the number of messages you sent to the server, and *sent* is the number of messages that were
successfully sent along to Druid. This may be fewer than the number of messages received if some of them were dropped
due to e.g. being outside the windowPeriod.

If there is an error sending the data, you will get an HTTP error code (4xx or 5xx).

## Setup

### Getting Tranquility Server

Tranquility Server is included in the
[tranquility-distribution-0.7.0](http://static.druid.io/tranquility/releases/tranquility-distribution-0.7.0.tgz)
tarball. To use it, first download it and then unpack it into your directory of choice by running
`tar -xzf tranquility-distribution-0.7.0.tgz`.

### Configuration

Tranquility Server uses a YAML file for configuration. You can see an example in `conf/server.yaml.example` of the
tarball distribution. You can start off your installation by copying this example file to `conf/server.yaml`. The
YAML file has two sections:

1. `dataSources` - per dataSource configuration.
2. `properties` - general server properties that apply to all dataSources. See "Configuration reference" for details.

The dataSources key should contain a mapping of dataSource name to configuration. Each dataSource configuration
has two sections:

1. `spec` - a YAML representation of a [Druid ingestion spec](http://druid.io/docs/latest/ingestion/index.html). Note
that the `ioConfig` is expected to be of type `realtime` but otherwise empty (null firehose, null plumber). This is
because Tranquility supplies its own firehose and plumber.
2. `properties` - per dataSource properties. See "Configuration reference" for details.

### Running

If you've saved your configuration into `conf/server.yaml`, run the server with:

```bash
bin/tranquility server -configFile conf/server.yaml
```

## Configuration reference

With the exception of the global properties (which must be provided at the top level), most properties can be provided
at the top level or per-dataSource. If the same property is provided at both levels, the per-dataSource version will
take precedence. This provides a way to set default properties that apply to all dataSources, while still allowing
customization for certain dataSources.

### Global properties

|Property|Description|Default|
|--------|-----------|-------|
|`http.port`|Port to listen on.|8200|
|`http.threads`|Number of threads for HTTP requests.|8|
|`http.idleTimeout`|Abort connections that have had no activity for longer than this timeout. Set to zero to disable. ISO8601 duration.|PT5M|

### Other properties

|Property|Description|Default|
|--------|-----------|-------|
|`druid.discovery.curator.path`|Curator service discovery path.|/druid/discovery|
|`druid.selectors.indexing.serviceName`|The druid.service name of the indexing service Overlord node.|druid/overlord|
|`task.partitions`|Number of Druid partitions to create.|1|
|`task.replicants`|Number of instances of each Druid partition to create. This is the *total* number of instances, so 2 replicants means 2 tasks will be created.|1|
|`task.warmingPeriod`|If nonzero, create Druid tasks early. This can be useful if tasks take a long time to start up in your environment.|PT0M|
|`zookeeper.connect`|ZooKeeper connect string.|none; must be provided|
|`zookeeper.timeout`|ZooKeeper session timeout. ISO8601 duration.|PT20S|
|`tranquility.maxBatchSize`|Maximum number of messages to send at once.|2000|
|`tranquility.maxPendingBatches`|Maximum number of batches that may be in flight before we block and wait for one to finish.|5|
|`tranquility.lingerMillis`|Wait this long for batches to collect more messages (up to maxBatchSize) before sending them. Set to zero to disable waiting.|0|
|`druidBeam.firehoseGracePeriod`|Druid indexing tasks will shut down this long after the windowPeriod has elapsed.|PT5M|
|`druidBeam.firehoseQuietPeriod`|Wait this long for a task to appear before complaining that it cannot be found.|PT1M|
|`druidBeam.firehoseRetryPeriod`|Retry for this long before complaining that events could not be pushed|PT1M|
|`druidBeam.firehoseChunkSize`|Maximum number of events to send to Druid in one HTTP request.|1000|
|`druidBeam.randomizeTaskId`|True if we should add a random suffix to Druid task IDs. This is useful for testing.|false|
|`druidBeam.indexRetryPeriod`|If an indexing service overlord call fails for some apparently-transient reason, retry for this long before giving up.|PT1M|
|`druidBeam.firehoseBufferSize`|Size of buffer used by firehose to store events.|100000|
