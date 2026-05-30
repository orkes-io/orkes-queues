# Queue benchmarks

Benchmark and load-test harnesses for the Redis-backed queue. They live in the test source set but
are **not** unit tests: every one is gated behind an opt-in system property and is skipped during a
normal `./gradlew test` / CI run. They exercise the **actual production classes**
(`ConductorRedisQueue` / `QueueMonitor`), so the numbers reflect real behavior.

These are what the performance work in this module was developed and validated against.

## Prerequisites

A Redis reachable at `localhost:6399` (default). No Docker/Testcontainers needed.

```bash
# start a throwaway Redis for benchmarking
redis-server --port 6399 --save "" --appendonly no --daemonize yes

# (optional) point the harnesses elsewhere
#   -Dbench.redis.host=<host> -Dbench.redis.port=<port>
```

Each harness reuses Redis state per run; they namespace keys by a random run id and/or `flush`
between phases, but starting from a clean Redis (`redis-cli -p 6399 flushall`) gives the cleanest
numbers.

All harnesses honor `--rerun-tasks` (Gradle caches test results, so re-running the same harness
without it is a no-op) and `-Dbench.out=<file>` to also write the report to a file.

## The harnesses

| Harness | Gate | What it measures |
|---|---|---|
| `QueueBenchmark` | `-Dbench=true` | Scoring micro-benchmark (`BigDecimal` vs primitive) **and** an end-to-end single-queue load test (throughput, latency, `evalsha` count, duplicates). |
| `FanoutBench` | `-Dbench.fanout=true` | Many queues, one worker each (the canonical Conductor topology), in one JVM. Public-API only, so it runs on `main` too. |
| `FanoutCluster` | `-Dbench.cluster=true` | **Cross-JVM** fan-out: producer in a child JVM, consumers here — the only way to measure true cross-process delivery latency (and the `BLPOP` doorbell). |
| `BenchCompare` | `-Dbench.cmp=true` | Portable, public-API-only fan-out for an apples-to-apples **`main` vs branch** comparison; reads Redis ops from `INFO commandstats`. |
| `LocalRedisFunctionalTest` | `-Dbench=true` | Not a benchmark — runs the full correctness suite against a local Redis (no containers). Use it to confirm a change is still correct. |

### `QueueBenchmark` — single queue, scoring + load

```bash
./gradlew :orkes-conductor-queues:test --tests '*QueueBenchmark*' -Dbench=true --rerun-tasks
```

Knobs: `bench.threads` (24 consumer threads), `bench.popBatch` (20), `bench.producerBatch` (50),
`bench.batchAck` (use `ackAll` instead of per-message `ack`), `bench.noAck` (skip acks — measures
the pop ceiling), `bench.latency=true` (run the latency probe variant with `bench.latency.consumers`
/ `.wait` / `.interval`).

### `FanoutBench` — N queues × 1 worker, one JVM

```bash
./gradlew :orkes-conductor-queues:test --tests '*FanoutBench*' -Dbench.fanout=true --rerun-tasks
```

Knobs (all `bench.fanout.*`): `queues` (1000), `workersPerQueue` (1), `batch` (10), `wait` ms (100),
`ratePerQueue` msgs/sec/queue (10), `publishers` (4), `pool` (512), `pollerThreads` (64),
`warmupMs` (3000), `measureMs` (10000).

### `FanoutCluster` — cross-JVM, doorbell

Spawns `FanoutClusterPublisher` as a child JVM so the producer and consumer are in **different
processes** (in-process push-wake does not apply — this is the real cross-instance case).

```bash
# baseline: timed-poll delivery (this is how main behaves cross-process)
./gradlew :orkes-conductor-queues:test --tests '*FanoutCluster' -Dbench.cluster=true --rerun-tasks

# event-driven BLPOP doorbell with 8 sharded listeners
./gradlew :orkes-conductor-queues:test --tests '*FanoutCluster' -Dbench.cluster=true \
    -Dbench.cluster.realDoorbell=true -Dbench.cluster.shards=8 --rerun-tasks
```

Knobs (all `bench.cluster.*`): `queues` (100), `workersPerQueue` (2), `batch` (10), `wait` ms (100),
`ratePerQueue` (10), `publishers` (4), `pool` (512), `pollerThreads` (64), `warmupMs` (4000),
`measureMs` (10000), `shards` (sharded `BLPOP` listener count), `realDoorbell` (end-to-end doorbell
through the queue), `doorbell` (direct `BLPOP`-latency micro-mode, bypasses the queue).

### `BenchCompare` — apples-to-apples `main` vs branch

Uses only the public `push`/`pop`/`ack` API and the 3-arg `ConductorRedisQueue` constructor, so the
**identical file compiles and runs on both** this branch and the pre-optimization `main`. Redis op
counts come from `INFO commandstats`, so they are codebase-agnostic.

```bash
# canonical (unsaturated): 20 queues x 1 worker @ 10 msg/s/queue
./gradlew :orkes-conductor-queues:test --tests '*BenchCompare' -Dbench.cmp=true \
    -Dbench.cmp.queues=20 -Dbench.cmp.workersPerQueue=1 -Dbench.cmp.ratePerQueue=10 --rerun-tasks

# hot queue (saturation): 1 queue x 32 workers, producers keep a backlog
./gradlew :orkes-conductor-queues:test --tests '*BenchCompare' -Dbench.cmp=true \
    -Dbench.cmp.queues=1 -Dbench.cmp.workersPerQueue=32 -Dbench.cmp.ratePerQueue=0 --rerun-tasks
```

Knobs (all `bench.cmp.*`): `queues` (100), `workersPerQueue` (1), `batch` (10), `wait` ms (100),
`ratePerQueue` (10; `0` = saturate), `publishers` (4), `pool` (512), `pollerThreads` (64),
`warmupMs` (4000), `measureMs` (10000), `backlogCap` (20000, saturate mode).

To run it against `main` for a before/after:

```bash
git worktree add /tmp/main-baseline origin/main
cp orkes-conductor-queues/src/test/java/io/orkes/conductor/mq/bench/BenchCompare.java \
   /tmp/main-baseline/orkes-conductor-queues/src/test/java/io/orkes/conductor/mq/bench/
# main's build.gradle does not forward -Dbench.* — add a forwarding block to its `test {}` task,
# or copy this module's build.gradle test block. Then run the same command above in the worktree.
```

## Tuning knobs (production `QueueMonitor`)

These configure the live `QueueMonitor` and can be passed to any harness to explore the trade-offs:

| Property | Default | Effect |
|---|---|---|
| `orkes.queue.maxPollers` | 8 | Max concurrent pollers per queue (parallelizes the Redis round-trip on a hot queue). |
| `orkes.queue.demandPerPoller` | 32 | Outstanding demand at which another poller is added. |
| `orkes.queue.maxCacheDepth` | 64 | Max messages cached ahead of consumption. Lower = lower hot-queue latency, more polls; higher = fewer polls, more buffering latency. Does not bind on sparse/canonical queues. |
| `orkes.queue.batchGatherMs` | 2 | After the first message, how long `pop` lingers gathering the rest of a batch before returning. |
| `orkes.queue.batchWindowMs` | 5 | Push-wake coalescing window (bounds push-driven polling on a hot queue). |
| `orkes.queue.maxPollBackoffMs` | 50 | Idle backoff cap used until a caller's `waitTime` is known. |

## Reading the output

Each harness prints a block to stdout (and to `-Dbench.out=<file>` if set). Common fields:

- **throughput** — messages consumed/sec during the measured window.
- **queue wait latency** — wall-clock ms from `push` to the consumer receiving the message
  (percentiles). In cross-JVM harnesses this is the real cross-process delivery latency.
- **redis ops / evalsha** — Redis-side work: total commands and the poll (`evalsha`) count. Fewer
  `evalsha` per message = less Redis load.
- **empty pops** — `pop` calls that returned nothing (wasted round-trips on the consumer side).
- **duplicates** — messages delivered more than once (should always be 0).

Absolute throughput is bound by the host (cores, local Redis); for conclusions, compare two runs on
the **same** machine with identical knobs and look at the relative difference.
