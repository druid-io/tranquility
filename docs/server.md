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

Tranquility Server is included in the [downloadable distribution](../README.md#downloadable-distribution).

### Configuration

Tranquility Server uses a standard [Tranquility configuration file](configuration.md) with some extra properties.
There's an example file at `conf/server.json.example` of the tarball distribution. You can start off your installation
by copying that file to `conf/server.json` and customizing it for your own setup.

These Server-specific properties, if used, must be specified at the global level:

|Property|Description|Default|
|--------|-----------|-------|
|`http.port`|Port to listen on.|8200|
|`http.threads`|Number of threads for serving HTTP requests.|40|
|`http.idleTimeout`|Abort connections that have had no activity for longer than this timeout. Set to zero to disable. ISO8601 duration.|PT5M|

### Running

If you've saved your configuration into `conf/server.json`, run the server with:

```bash
bin/tranquility server -configFile conf/server.json
```
