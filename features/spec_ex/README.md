## Speculative query execution

Sometimes a Cassandra node might be experiencing difficulties (ex: long
GC pause) and take longer than usual to reply. Queries sent to that node
will experience bad latency.

One thing we can do to improve that is pre-emptively start a second
execution of the query against another node, before the first node has
replied or errored out. If that second node replies faster, we can send
the response back to the client (we also cancel the first query):

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                |     host2 replies   |
  |                |<--------------------|
  |   complete     |              |
  |<---------------|              |
  |                | cancel       |
  |                |------------->|
```

Or the first node could reply just after the second execution was
started. In this case, we cancel the second execution. In other words,
whichever node replies faster "wins" and completes the client query:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

Speculative executions are **disabled** by default. The following
sections cover the practical details and how to enable them.

### Query idempotence

One important aspect to consider is whether queries are idempotent, i.e.
whether they can be applied multiple times without changing the result
beyond the initial application. **If a query is not idempotent, the
driver will never schedule speculative executions for it**, because
there is no way to guarantee that only one node will apply the mutation.

As of Cassandra 2.1.4, the only queries that are *not* idempotent are:

* counter operations;
* prepending or appending to a list column.

In the driver, this is determined by `Statement#isIdempotent()`.
Unfortunately, the driver doesn't parse query strings, so in most cases
it has no information about what the query actually does. Therefore:

* **`Statement#isIdempotent()` is only computed automatically for
  statements built with `QueryBuilder`**;
* **for all other types of statements, it defaults to `false`.** You'll
  need to set it manually, with one of the mechanism described below.

You can override the value on each statement:

```java
Statement s = new SimpleStatement("SELECT * FROM users WHERE id = 1");
s.setIdempotent(true);
```

Note that this will also work for built statements (and override the
computed value).

Additionally, if you know for a fact that your application does not use
counters nor list appends/prepends, you can change the default
cluster-wide:

```java
// Make all statements idempotent by default:
cluster.getConfiguration().getQueryOptions().setDefaultIdempotence(true);
```

### Configuring a `SpeculativeExecutionPolicy`

Speculative executions are controlled by a pluggable policy that is
specified when initializing the `Cluster` instance. A simple
implementation based on constant delays is provided with the driver:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withSpeculativeExecutionPolicy(
        new ConstantSpeculativeExecutionPolicy(
            2,   // maximum number of executions
            500  // delay before a new execution is launched
        ))
    .build();
```

Given the above configuration, an idempotent query would be handled this
way:

* start the initial execution at t0;
* if no response has been received at t0 + 500 milliseconds, start a
  speculative execution on another node;
* if no response has been received at t0 + 1000 milliseconds, start
  another speculative execution on a third node.

You can write your own policy by implementing `SpeculativeExecution`.
There are plans to provide a more elaborate, percentile-based policy
in a future driver version, see
[JAVA-723](https://datastax-oss.atlassian.net/browse/JAVA-723) for more
information.

### How speculative executions affect retries

Regardless of speculative executions, the driver has a retry mechanism:

* on an internal error, it will try the next host;
* when the consistency level could not be reached (`unavailable` error
  or read or write timeout), it will delegate the decision to
  `RetryPolicy`, which might trigger a retry on the same host.

Turning speculative executions on doesn't change this behavior. Each
parallel execution will trigger retries independently:

```ditaa
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                | unavailable  |
  |                |<-------------|
  |                |
  |                |retry at lower CL
  |                |------------->|
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |     server error    |
  |                |<--------------------|
  |                |              |
  |                |   retry on host3
  |                |-------------------->|
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

The only impact is that all executions of the same query always share
the same query plan, so each host will be used by at most one execution.

### Tuning and practical details

The goal of speculative executions is to improve overall latency (the
time between `execute(query)` and `complete` in the diagrams above) at
high percentiles. On the flipside, they cause the driver to send more
individual requests, so throughput will not necessarily improve.

You can monitor how many speculative executions were triggered with the
`speculative-executions` metric (exposed in the Java API as
`cluster.getMetrics().getErrors().getSpeculativeExecutions()`). It
should only be a few percents of the total number of requests
(`cluster.getMetrics().getRequestsTimer().getCount()`).

#### Stream id exhaustion

One side-effect of speculative executions is that many requests are
cancelled, which can lead to a phenomenon called *stream id exhaustion*:
each TCP connection can handle multiple simultaneous requests,
identified by a unique number called *stream id*. When a request gets
cancelled, we can't reuse its stream id immediately because we might
still receive a response from the server later. If this happens often,
the number of available stream ids diminishes over time, and when it
goes below a given threshold we close the connection and create a new
one. If requests are often cancelled, so will see connections being
recycled at a high rate.

One way to detect this is to monitor open connections per host
(`Session.getState().getOpenConnections(host)`) against TCP connections
at the OS level. If open connections stay constant but you see many TCP
connections in closing states, you might be running into this issue. Try
raising the speculative execution threshold.

This problem is more likely to happen with version 2 of the native
protocol, because each TCP connection only has 128 stream ids. With
version 3 (driver 2.1.2 or above with Cassandra 2.1 or above), there are
32K stream ids per connection, so higher cancellation rates can be
sustained. If you're unsure of which native protocol version you're
using, you can check with
`cluster.getConfiguration().getProtocolOptions().getProtocolVersion()`.

#### Request ordering and client timestamps

Another issue that might arise is that you get unintuitive results
because of request ordering. Suppose you run the following query with
speculative executions enabled:

    insert into my_table (k, v) values (1, 1);

The first execution is a bit too slow, so a second execution gets
triggered. Finally, the first execution completes, so the client code
gets back an acknowledgement, and the second execution is cancelled.
However, cancelling only means that the driver stops waiting for the
server's response, the request could still be "on the wire"; let's
assume that this is the case.

Now you run the following query, which completes successfully:

    delete from my_table where k = 1;

But now the second execution of the first query finally reaches its
target node, which applies the mutation. The row that you've just
deleted is back!

The workaround is to use a timestamp with your queries:

    insert into my_table (k, v) values (1, 1) USING TIMESTAMP 1432764000;

If you're using native protocol v3, you can also enable client-side
timestamps to have this done automatically; see
[this blog post](http://www.datastax.com/dev/blog/java-driver-2-1-2-native-protocol-v3)
for more information.
