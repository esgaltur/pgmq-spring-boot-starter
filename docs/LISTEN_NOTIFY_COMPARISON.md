# LISTEN/NOTIFY comparison

The starter supports two listener modes against the same durable PGMQ queues:

```yaml
spring:
  pgmq:
    listener-mode: notify # default
    notification-recovery-interval: 30s
    notification-reconnect-interval: 1s
    notification-throttle-interval: 250ms
    auto-enable-notifications: true
```

## What notification mode does—and does not—replace

Notification mode uses PostgreSQL's native `LISTEN/NOTIFY` mechanism as a
**wake-up signal**. It does not use a notification as the message and does not
replace the PGMQ extension. PGMQ remains the durable data plane responsible for
message storage, visibility timeouts, reads, retries, and archiving. A
notification only tells a listener that it is worth checking a queue.

This separation is what makes the mode safe to use: PostgreSQL notifications
are transient, but PGMQ messages are durable. If a notification is missed while
the connection is unavailable, the message remains in its queue and is found by
the next recovery scan. A missed notification can increase latency; it does not
discard the queued message.

### Can native LISTEN/NOTIFY remove the PGMQ extension dependency?

Not by itself. PostgreSQL notifications provide neither durable storage nor
competing-consumer queue semantics. They do not provide message claiming,
acknowledgement, visibility timeouts, retry counts, delayed delivery, archiving,
or replay for a consumer that was offline. They are broadcast to sessions that
are listening at delivery time.

The current implementation still calls PGMQ operations such as `send`, `read`,
`set_vt`, `archive`, and `delete`; notification setup also uses PGMQ's
`enable_notify_insert` function. Therefore, this mode improves how a PGMQ
consumer waits, but it does not make the starter independent of PGMQ.

Removing the extension would require a separate durable queue implementation:
starter-managed PostgreSQL tables, transactional claim operations (commonly
built with `FOR UPDATE SKIP LOCKED`), retry and visibility-timeout state, and
archive/delete operations. Native `LISTEN/NOTIFY` could still wake those
consumers, but the starter would then own the queue behavior currently supplied
and maintained by PGMQ.

Using a notification payload as the message is appropriate only for best-effort
events such as cache invalidation or live UI refresh, where loss during a
disconnect is acceptable. It is not a safe replacement for durable background
jobs, transactional outbox events, payments, or other work that must eventually
be processed.

Set `listener-mode: polling` to retain the previous fixed-delay behavior. In
notification mode, each worker drains the queue until it is empty and then waits
for PGMQ's `pgmq.q_<queue>.INSERT` notification. A timed recovery scan remains
necessary for reconnect gaps, delayed messages, and visibility-timeout retries.
After a notification, workers also perform one confirmation scan at the end of
the throttle interval so inserts coalesced into the burst cannot be stranded.

## Is it usable?

Yes, for applications that already use PGMQ and run continuously. Notification
mode is particularly useful when queues are often empty and work should begin
quickly after a transaction commits. The implementation keeps polling as a
safety mechanism and automatically uses it for queues whose notification setup
fails.

The project is still in early beta, so production adopters should validate
their own peak write rate, application replica count, connection budget, and
database failover behavior. For mission-critical workloads, retain metrics and
alerts for queue depth and processing latency, and test that recovery scans meet
the acceptable delay during a simulated LISTEN disconnect.

## Recommended use cases

### Transactional outbox events

An application writes domain data and sends a PGMQ message in the same database
transaction. PostgreSQL delivers the notification after commit, so a listener
can react promptly while the PGMQ row remains the durable source of truth.
Examples include order fulfillment, account provisioning, audit propagation,
and cache invalidation.

### Sparse background jobs

Email, webhook, document-generation, and image-processing queues may be idle for
long periods. Notification mode avoids repeated empty reads while reducing the
wait before a newly submitted job starts.

### Moderate traffic with latency targets

Queues with intermittent bursts benefit from notification throttling: one
signal can wake workers that then drain the available backlog. This fits
always-on services where tens or hundreds of milliseconds matter more than
maximizing producer throughput at all costs.

### Small or moderate numbers of consumer instances

Each application instance owns one dedicated LISTEN connection, regardless of
its number of listener methods or worker threads. This is a good fit when the
database connection budget can accommodate one additional session per
instance.

## Prefer polling when

- A queue is permanently busy. Workers already keep draining it, so
  notifications provide little latency benefit while retaining trigger cost.
- Producer throughput is the main constraint. The reference benchmark below
  measured an additional 93 ms for a 10,000-message batch with the notification
  trigger enabled.
- The deployment has many consumer replicas. PostgreSQL broadcasts each
  notification to every listening instance, which can create competing empty
  reads after one instance has claimed the available messages.
- Database connections are scarce. Notification mode reserves one additional
  session for each application instance.
- Most work is delayed or consists of visibility-timeout retries. These events
  do not necessarily produce a new insert notification and are discovered by
  the recovery scan; polling may provide a clearer latency bound.
- The database does not provide PGMQ 1.10+, or the application is not permitted
  to install/manage the notification trigger.
- Consumers run only intermittently, such as request-scoped serverless
  functions. Use synchronous `PgmqTemplate` reads from an external scheduler
  instead of a background listener.

## Mode selection guide

| Primary concern | Recommended mode | Reason |
|---|---|---|
| Low latency on an often-empty queue | `notify` | Immediate signal without continuous empty reads |
| Transactional outbox handling | `notify` | Listener wakes after committed inserts |
| Intermittent bursts | `notify` | Signal starts queue draining; throttling coalesces inserts |
| Permanently backlogged queue | `polling` | There is no idle wait for a notification to improve |
| Maximum batch-ingest throughput | `polling` | Avoids the insert-trigger overhead |
| Very large consumer fleet | Benchmark both | Broadcast wake-ups may cause excess competing reads |
| Strict database connection limit | `polling` | Does not reserve a dedicated LISTEN session |
| Delay/retry-heavy workload | `polling`, or shorter recovery interval | Delayed visibility is found by timed reads |

## Operational constraints

- `LISTEN` is session-scoped. The dedicated connection must remain pinned to
  one PostgreSQL session. Do not route it through PgBouncer transaction pooling;
  use a direct connection or session pooling for this datasource.
- With `auto-enable-notifications: true`, the starter asks PGMQ to install the
  insert notification trigger. Production environments can manage that trigger
  in Flyway or Liquibase and set the property to `false`.
- `notification-recovery-interval` is the upper latency bound for missed
  signals, delayed messages, and visibility-timeout retries during otherwise
  idle periods. Lower values recover faster but perform more empty reads.
- `notification-throttle-interval` trades producer overhead and signal volume
  for wake-up latency during bursts.
- Notifications contain no application payload in this design. Consumers
  always load the authoritative message from PGMQ.

## Expected trade-offs

| Workload | Polling | LISTEN/NOTIFY |
|---|---|---|
| Sparse messages | Latency up to the poll interval; repeated empty reads | Immediate wake-up; almost no empty reads |
| Sustained backlog | Continuously drains | Continuously drains; notification is off the hot path |
| Insert cost | Baseline | Small trigger/notification overhead, throttled by PGMQ |
| Connections | Consumer queries only | One additional dedicated LISTEN connection per application instance |
| Failure recovery | Next poll | Reconnect plus timed recovery scan |

## Reproducible comparison

The opt-in Testcontainers comparison measures controlled low-traffic wake-up
latency and a 10,000-message batch insert with and without the notification
trigger on the same PostgreSQL instance:

```powershell
mvn '-Dpgmq.benchmark=true' '-Dtest=PgmqListenerModeComparisonTest' test
```

The comparison uses `ghcr.io/pgmq/pg18-pgmq:v1.10.0`. Its timings are intended
for comparing modes on the same machine, not as universal throughput claims.

## Reference result

Measured on 2026-08-22 using PostgreSQL 18/PGMQ 1.10 in Docker Desktop:

| Measurement | Polling/no trigger | LISTEN/NOTIFY trigger |
|---|---:|---:|
| Controlled sparse wake-up, median | 264 ms | 17 ms |
| Controlled sparse wake-up, p95 | 274 ms | 18 ms |
| `send_batch` of 10,000 messages, median of 5 warmed alternating runs | 54 ms | 147 ms |

The latency comparison deliberately sends immediately after an empty polling
read with a 250 ms polling interval, so it represents the controlled near-worst
case rather than average uniformly distributed polling latency.

The trigger added 93 ms to a 10,000-message batch in this environment, roughly
9.3 microseconds per inserted message. This confirms PGMQ's guidance: use
notifications for sparse or latency-sensitive queues; pure polling can be the
better choice for producers saturating a permanently busy queue. Consumption
throughput after wake-up uses the same `pgmq.read` path in both modes.
