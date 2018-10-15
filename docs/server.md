# Server

Tranquility Server lets you use Tranquility to send data to Druid without developing a JVM app. You only need an
HTTP client. Tranquility Server is easy to set up: a single server process can handle multiple dataSources, and you can
scale out by simply starting more server processes and putting them behind a load balancer or DNS round-robin.

## HTTP API

You can send data through HTTP POST. You can send as little or as much data at once as you like.

The form of the request is:

- POST to `/v1/post/DATASOURCE`.
- Set `Content-Type: text/plain`.
- Body should be newline-delimited text data matching the parser you provided in your [server.json](#configuration).

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

This API is synchronous and will block until your messages are either sent to Druid, or have failed to send.

If there is an error sending the data, you will get an HTTP error code (4xx or 5xx).

### Request compression

Tranquility Server supports request compression. To use request compression, set the "Content-Encoding" header of your
request and compress your payload. Currently supported options are:

- gzip: Same scheme as HTTP's gzip response encoding.
- identity: No compression.

### Direct object option

You can also POST objects directly to Tranquility Server without going through the string-oriented parser. To do this,
use a request with this form:

- POST to `/v1/post` or `/v1/post/{DATASOURCE}`.
- Set `Content-Type: application/json` or `Content-Type: application/x-jackson-smile`.
- Body should be JSON or Smile data matching the Content-Type you provided. If JSON, you can provide either an array
of JSON objects or a sequence of newline-delimited JSON objects. If Smile, you should provide an array of objects.
- If using the path `/v1/post`, your objects must have a field named "dataSource" or "feed" containing the destination
dataSource for that object.

In this case, the parser provided in your [server.json](#configuration) is still used to extract the timestamp and
dimensions from your objects, but is not used to parse your objects.

### Asynchronous option

Tranquility Server also offers an asynchronous option. Just add "?async=true" to the URL. Note that "sent" in the
response will always be 0 when using the asynchronous option.

If the server's outgoing message queue fills up, your asynchronous requests will either block or fail. To control this
behavior, set the `tranquility.blockOnFull` property to either true or false. If you set this to "false" and your
server's outgoing queue is full, you will get an HTTP 503.

With both the synchronous and asynchronous modes, you can adjust the size of your Server's message queues through
the properties `tranquility.maxBatchSize` and `tranquility.maxPendingBatches`.

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
|`http.port`|Port to listen on, with plaintext traffic|8200|
|`http.port.enable`|Enable the port for plaintext traffic.|true|
|`https.port`|Port to listen on, with TLS.|8400|
|`https.port.enable`|Enable the TLS port.|false|
|`https.keyStorePath`|Path of the keystore file that contains Traquility Server's TLS certificate.|''|
|`https.keyStorePassword`|Password for the keystore|''|
|`https.certAlias`|Alias of the server certificate in the keystore|''|
|`https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|KeyManagerFactory.getDefaultAlgorithm()|
|`https.keyManagerPassword`|Password for the KeyManager|''|
|`https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|
|`https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|
|`https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|
|`https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|
|`http.threads`|Number of threads for serving HTTP requests.|40|
|`http.idleTimeout`|Abort connections that have had no activity for longer than this timeout. Set to zero to disable. ISO8601 duration.|PT5M|

### Running

If you've saved your configuration into `conf/server.json`, run the server with:

```bash
bin/tranquility server -configFile conf/server.json
```
