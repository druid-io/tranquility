# Overview

Tranquility is a Scala library that coordinates streaming ingestion through the
[Druid indexing service](http://druid.io/docs/latest/design/indexing-service.html). It exists because the Druid
indexing service API is fairly low level. In the indexing service, callers create "tasks" that run on a single machine
using a fixed bundle of resources. Indexing tasks start up with a particular configuration and generally this
configuration cannot be changed once the task is running.

Tranquility manages creation of Druid indexing tasks, handling partitioning, replication, service discovery, and schema
rollover for you, seamlessly and without downtime. You never have to write code to deal with individual tasks directly.

## Task creation

Normally, Tranquility spawns one task per [Druid segment](http://druid.io/docs/latest/design/segments.html). Each task
is a realtime index task (type *index_realtime*) based around an EventReceiverFirehose, which exposes an HTTP endpoint
that accepts POSTed events. Each task is associated with a particular interval (based on your segmentGranularity)
and a particular partitionNum. Tasks are generally created when the first data is seen for a particular time window.

The firehose is decorated with a *clipped* firehose that ensures data outside the segment's interval is dropped, and
a *timedShutoff* firehose that ensures the task ends soon after the segment's interval is over. The rejectionPolicy
is normally *none*; Tranquility drops events outside the windowPeriod on the client side, without ever sending them to
a Druid task.

After the timed shutoff occurs, each task will push its segment to deep storage and wait for handoff (i.e. wait for
historical nodes to load it). After this occurs, the task will exit. Therefore each task lives for roughly the
segmentGranularity period + the windowPeriod + however long pushing takes + however long handoff takes. Generally
pushing and handoff should take a couple of minutes each in a smoothly running cluster.

Tranquility coordinates all task creation through ZooKeeper. You can start up as many Tranquility instances as you
like with the same configuration, even on different machines, and they will send to the same set of tasks.

There is one exception to the one-task-one-segment rule. Tranquility will spawn a task that handles multiple segments
if you're using the `maxSegmentsPerBeam` option. In this situation, the rejectionPolicy is set to *server* to ensure
prompt handoff.

## Segment granularity and window period

The segmentGranularity is the time period covered by the segments produced by each task. For example, a
segmentGranularity of "hour" will spawn tasks that create segments covering one hour each.

The windowPeriod is the slack time permitted for events. For example, a windowPeriod of ten minutes (the default) means
that any events with a timestamp older than ten minutes in the past, or more than ten minutes in the future, will be
dropped.

These are important configurations because they influence how long tasks will be alive for, and how long data stays
in the realtime system before being handed off to the historical nodes. For example, if your configuration has
segmentGranularity "hour" and windowPeriod ten minutes, tasks will stay around listening for events for an hour and ten
minutes. For this reason, to prevent excessive buildup of tasks, it is recommended that your windowPeriod be less than
your segmentGranularity.

## Partitioning and replication

Tranquility implements partitioning by creating multiple tasks, each with a different shardSpec partitionNum. Events
are partitioned according to a Partitioner or beam merging function that you can provide. The default Partitioner
works only on Maps and partitions them by time and dimensions to get the best rollup.

Replication involves creating multiple tasks with the _same_ partitionNum. Events destined for a particular partition
are sent to all replica tasks at once. The tasks do not communicate with each other. If a replica is lost, it cannot
be replaced, since Tranquility does not have any way of replaying previously sent data. Therefore the system will run
with one less replica than configured, until the next segmentGranularity interval window starts. At this point,
Tranquility will create a new set of tasks with the expected number of replicas.

## Schema evolution

Tranquility implements schema evolution (changing dimensions and metrics) by applying any new configuration to new
tasks. Previously-created tasks continue using the old configuration. In practice, this means schema changes can take
effect at the start of each segmentGranularity period.

## Druid setup

To get started with the Druid indexing service, you'll need an Overlord, enough Middle Managers for your realtime
workload, and enough historical nodes to receive handoffs. You don't need any realtime nodes, since Tranquility uses
the indexing service for all of its ingestion needs.

Tranquility periodically submits new tasks to the indexing service. These new tasks are typically submitted before the
old ones exit, so to allow for smooth transitions, you'll need enough indexing service worker capacity to run two sets
of overlapping tasks (that's 2 * #partitions * #replicants). The number of partitions and replicants both default to 1
(single partition, single copy).

## Guarantees

Tranquility operates under a best-effort design. It tries reasonably hard to preserve your data, by allowing you to
set up replicas and by retrying failed pushes for a period of time, but it does not guarantee that your events will be
processed exactly once. In some conditions, it can drop or duplicate events:

- Events with timestamps outside your configured windowPeriod will be dropped.
- If you suffer more Druid Middle Managers failures than your configured replicas count, some partially indexed data
may be lost.
- If there is a persistent issue that prevents communication with the Druid indexing service, and retry policies are
exhausted during that period, or the period lasts longer than your windowPeriod, some events will be dropped.
- If there is an issue that prevents Tranquility from receiving an acknowledgement from the indexing service, it will
retry the batch, which can lead to duplicated events.
- If you are using Tranquility inside Storm or Samza, various parts of the both architectures have an at-least-once
design and can lead to duplicated events.

Our approach at Metamarkets is to send all of our data through Tranquility in real-time, but to also mitigate these
risks by storing a copy in S3 and following up with a nightly Hadoop batch indexing job to re-ingest the data. This
allow us to guarantee that in the end, every event is represented exactly once in Druid. The setup mitigates other risks
as well, including the fact that data can come in later than expected (if a previous part of the pipeline is delayed
by more than the windowPeriod) or may need to be revised after initial ingestion.
