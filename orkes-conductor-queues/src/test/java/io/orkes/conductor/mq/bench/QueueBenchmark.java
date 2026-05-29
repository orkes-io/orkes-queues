/*
 * Copyright 2026 Orkes, Inc.
 * <p>
 * Licensed under the Orkes Community License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * https://github.com/orkes-io/licenses/blob/main/community/LICENSE.txt
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.orkes.conductor.mq.bench;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

/**
 * Standalone benchmark harness for the Redis-backed queue. NOT a unit test — it is skipped unless
 * run with {@code -Dbench=true} and a Redis reachable at {@code bench.redis.host:bench.redis.port}
 * (defaults {@code localhost:6399}).
 *
 * <p>Run: {@code ./gradlew :orkes-conductor-queues:test --tests '*QueueBenchmark*' -Dbench=true}
 *
 * <p>It measures two things, on the <b>actual</b> production classes:
 *
 * <ol>
 *   <li><b>#2 scoring</b> — an A/B microbenchmark of {@code BigDecimal} vs primitive-double score
 *       encode+decode, run in the same JVM for an apples-to-apples comparison, plus an equivalence
 *       check.
 *   <li><b>#1 refill stampede</b> — an end-to-end load test that records throughput, pop latency
 *       percentiles, the Redis-side {@code evalsha} call count, and duplicate deliveries.
 * </ol>
 */
public class QueueBenchmark {

    private static final String HOST = System.getProperty("bench.redis.host", "localhost");
    private static final int PORT = Integer.getInteger("bench.redis.port", 6399);

    // ---- end-to-end load parameters ----
    private static final int CONSUMER_THREADS = Integer.getInteger("bench.threads", 24);
    private static final int POP_BATCH = 10;
    private static final int POP_WAIT_MS = 5;
    private static final int WARMUP_MS = 2_000;
    private static final int MEASURE_MS = 8_000;
    private static final int PRODUCER_BATCH = 50;

    private static final MathContext PRECISION_MC = new MathContext(20);
    private static final BigDecimal HUNDRED = new BigDecimal(100);

    @Test
    public void benchmark() {
        Assumptions.assumeTrue(
                Boolean.getBoolean("bench"), "set -Dbench=true to run the benchmark");
        assumeRedisReachable();

        StringBuilder report = new StringBuilder();
        report.append("\n==================== QUEUE BENCHMARK ====================\n");
        report.append("redis=").append(HOST).append(":").append(PORT).append("\n\n");

        if (Boolean.getBoolean("bench.latency")) {
            benchDeliveryLatency(report);
        } else {
            benchScoring(report);
            benchEndToEnd(report);
        }

        report.append("========================================================\n");
        System.out.println(report);
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of(System.getProperty("bench.out", "/tmp/bench-out.txt")),
                    report.toString());
        } catch (Exception ignored) {
        }
    }

    // ------------------------------------------------------------------
    // #3 — delivery-latency probe: how long from push() to a poller observing
    // the message, on a SPARSE queue with FEW consumers (where fixed-interval
    // idle polling hurts latency most). Also reports evalsha spent while idle.
    // ------------------------------------------------------------------
    private void benchDeliveryLatency(StringBuilder report) {
        int consumers = Integer.getInteger("bench.latency.consumers", 2);
        int waitMs = Integer.getInteger("bench.latency.wait", 50);
        int intervalMs = Integer.getInteger("bench.latency.interval", 100);

        String queueName = "benchlat_" + UUID.randomUUID();
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(consumers + 8);
        JedisPooled pooled = new JedisPooled(poolConfig, HOST, PORT);
        ExecutorService refillPool = Executors.newFixedThreadPool(2);
        ConductorRedisQueue queue =
                new ConductorRedisQueue(queueName, new UnifiedJedisCommands(pooled), refillPool);
        queue.flush();

        ExecutorService consumerPool = Executors.newFixedThreadPool(consumers);
        ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean running = new AtomicBoolean(true);
        List<long[]> latBuffers = new CopyOnWriteArrayList<>();
        AtomicLong delivered = new AtomicLong();

        // producer: one message every intervalMs, push time (nanos) encoded in the id
        producer.scheduleAtFixedRate(
                () -> {
                    String id = System.nanoTime() + ":" + UUID.randomUUID();
                    QueueMessage m = new QueueMessage(id, "");
                    m.setPriority(0);
                    try {
                        queue.push(List.of(m));
                    } catch (Exception ignored) {
                    }
                },
                0,
                intervalMs,
                TimeUnit.MILLISECONDS);

        Runnable consumerTask =
                () -> {
                    long[] lat = new long[1_000_000];
                    int n = 0;
                    while (running.get()) {
                        List<QueueMessage> popped = queue.pop(10, waitMs, TimeUnit.MILLISECONDS);
                        long observed = System.nanoTime();
                        for (QueueMessage m : popped) {
                            String id = m.getId();
                            int c = id.indexOf(':');
                            if (c > 0) {
                                try {
                                    long pushed = Long.parseLong(id.substring(0, c));
                                    if (n < lat.length) lat[n++] = observed - pushed;
                                    delivered.incrementAndGet();
                                } catch (NumberFormatException ignored) {
                                }
                            }
                            queue.ack(m.getId());
                        }
                    }
                    latBuffers.add(java.util.Arrays.copyOf(lat, n));
                };

        for (int i = 0; i < consumers; i++) consumerPool.submit(consumerTask);
        sleep(WARMUP_MS);

        // measurement window
        latBuffers.clear();
        delivered.set(0);
        running.set(false);
        sleep(waitMs + 50L);
        running.set(true);
        try (Jedis admin = new Jedis(HOST, PORT)) {
            admin.configResetStat();
        }
        long start = System.nanoTime();
        for (int i = 0; i < consumers; i++) consumerPool.submit(consumerTask);
        sleep(MEASURE_MS);
        double elapsedSec = (System.nanoTime() - start) / 1e9;
        running.set(false);
        producer.shutdownNow();
        consumerPool.shutdown();
        try {
            consumerPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        long evalsha = commandCalls("evalsha");
        int total = latBuffers.stream().mapToInt(a -> a.length).sum();
        long[] all = new long[total];
        int idx = 0;
        for (long[] b : latBuffers) {
            System.arraycopy(b, 0, all, idx, b.length);
            idx += b.length;
        }
        java.util.Arrays.sort(all);

        report.append("---- #3 delivery latency probe (push -> observed) ----\n");
        report.append(
                String.format(
                        "  consumers=%d  pop waitTime=%dms  arrival every %dms  duration=%.1fs%n",
                        consumers, waitMs, intervalMs, elapsedSec));
        report.append(String.format("  delivered:     %,d msgs%n", delivered.get()));
        report.append(
                String.format(
                        "  Redis evalsha: %,d  (idle polling cost; ~%.0f/sec)%n",
                        evalsha, evalsha / elapsedSec));
        report.append(
                String.format(
                        "  delivery latency (ms): p50=%.2f  p95=%.2f  p99=%.2f  max=%.2f%n",
                        pct(all, 50) / 1e6,
                        pct(all, 95) / 1e6,
                        pct(all, 99) / 1e6,
                        (all.length == 0 ? 0 : all[all.length - 1]) / 1e6));
        report.append("\n");

        queue.flush();
        refillPool.shutdownNow();
        pooled.close();
    }

    // ------------------------------------------------------------------
    // #2 — scoring micro A/B (BigDecimal vs primitive)
    // ------------------------------------------------------------------
    private void benchScoring(StringBuilder report) {
        final int iterations = 5_000_000;
        long now = System.currentTimeMillis();

        // correctness: proposed encode/decode must match current within tolerance
        long mismatches = 0;
        double maxDelta = 0;
        for (int p = 0; p <= 100; p++) {
            for (long t : new long[] {1, 30, 1000, 30_000, 3_600_000}) {
                double a = scoreBigDecimal(now, t, p);
                double b = scorePrimitive(now, t, p);
                double d = Math.abs(a - b);
                maxDelta = Math.max(maxDelta, d);
                if (d > 1e-3) mismatches++;
            }
        }

        // warmup
        blackhole = 0;
        runScore(true, 200_000, now);
        runScore(false, 200_000, now);

        long tBig = runScore(true, iterations, now);
        long tNew = runScore(false, iterations, now);

        double nsBig = tBig / (double) iterations;
        double nsNew = tNew / (double) iterations;

        report.append("---- #2 scoring encode+decode (").append(iterations).append(" ops) ----\n");
        report.append(
                String.format(
                        "  BigDecimal (current): %8.2f ns/op  (%,.0f ops/sec)%n",
                        nsBig, 1e9 / nsBig));
        report.append(
                String.format(
                        "  primitive (proposed): %8.2f ns/op  (%,.0f ops/sec)%n",
                        nsNew, 1e9 / nsNew));
        report.append(String.format("  speedup:              %8.2fx%n", nsBig / nsNew));
        report.append(
                String.format(
                        "  equivalence: maxScoreDelta=%.2e, mismatches(>1e-3)=%d%n%n",
                        maxDelta, mismatches));
        report.append("  (blackhole=").append(blackhole).append(")\n\n");
    }

    private volatile long blackhole;

    private long runScore(boolean bigDecimal, int iterations, long now) {
        long acc = 0;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            int priority = i % 101;
            long timeout = (i % 2 == 0) ? 30_000 : 0;
            double score =
                    bigDecimal
                            ? scoreBigDecimal(now, timeout, priority)
                            : scorePrimitive(now, timeout, priority);
            int decoded = bigDecimal ? decodeBigDecimal(score) : decodePrimitive(score);
            acc += decoded + (long) score;
        }
        long elapsed = System.nanoTime() - start;
        blackhole += acc;
        return elapsed;
    }

    /** Exact copy of the current ConductorQueue.getScore + QueueMonitor decode (BigDecimal). */
    private double scoreBigDecimal(long now, long timeout, int priority) {
        double score;
        if (timeout > 0) {
            BigDecimal to = new BigDecimal(now + timeout);
            BigDecimal divideByOne =
                    BigDecimal.ONE.divide(new BigDecimal(priority + 1), PRECISION_MC);
            BigDecimal oneMinusDivByOne = BigDecimal.ONE.subtract(divideByOne);
            score = to.add(oneMinusDivByOne).doubleValue();
        } else {
            score = priority > 0 ? priority : now;
        }
        return score;
    }

    private int decodeBigDecimal(double score) {
        return new BigDecimal(Double.toString(score))
                .remainder(BigDecimal.ONE)
                .multiply(HUNDRED)
                .intValue();
    }

    /** Proposed primitive-math equivalent. */
    private double scorePrimitive(long now, long timeout, int priority) {
        if (timeout > 0) {
            return (double) (now + timeout) + (1.0 - 1.0 / (priority + 1));
        }
        return priority > 0 ? priority : now;
    }

    private int decodePrimitive(double score) {
        return (int) ((score - Math.floor(score)) * 100);
    }

    // ------------------------------------------------------------------
    // #1 — end-to-end refill-stampede load test
    // ------------------------------------------------------------------
    private void benchEndToEnd(StringBuilder report) {
        // SPARSE: the real Conductor pattern — many workers polling a mostly-empty queue.
        // This is where the synchronous refill stampede (every pop on an empty cache fires its
        // own evalsha) dominates, so evalsha-per-pop is the headline metric.
        runScenario("SPARSE / empty queue (Conductor worker-poll pattern)", 0, report);
        // SATURATED: cache stays full; verifies throughput is not regressed by the guard.
        runScenario("SATURATED / high-rate producer", PRODUCER_BATCH, report);
    }

    /**
     * @param producerBatch messages pushed every ms by the producer; 0 means no producer (the queue
     *     stays empty and consumers poll into the void).
     */
    private void runScenario(String name, int producerBatch, StringBuilder report) {
        String queueName = "bench_" + UUID.randomUUID();
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(4);
        // Scale the pool with the poller count so connection starvation does not mask the
        // algorithmic behavior we are trying to measure.
        poolConfig.setMaxTotal(Math.max(64, CONSUMER_THREADS + 16));
        JedisPooled pooled = new JedisPooled(poolConfig, HOST, PORT);
        ExecutorService refillPool = Executors.newFixedThreadPool(2);
        ConductorRedisQueue queue =
                new ConductorRedisQueue(queueName, new UnifiedJedisCommands(pooled), refillPool);
        queue.flush();

        ExecutorService consumers = Executors.newFixedThreadPool(CONSUMER_THREADS);
        ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor();
        ConcurrentHashMap<String, Integer> seen = new ConcurrentHashMap<>();
        AtomicLong consumed = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        AtomicLong popCalls = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);
        List<long[]> latencyBuffers = new CopyOnWriteArrayList<>();

        if (producerBatch > 0) {
            producer.scheduleAtFixedRate(
                    () -> {
                        List<QueueMessage> batch = new ArrayList<>(producerBatch);
                        for (int i = 0; i < producerBatch; i++) {
                            QueueMessage m = new QueueMessage(UUID.randomUUID().toString(), "");
                            m.setPriority(0);
                            batch.add(m);
                        }
                        try {
                            queue.push(batch);
                        } catch (Exception ignored) {
                        }
                    },
                    0,
                    1,
                    TimeUnit.MILLISECONDS);
        }

        Runnable consumerTask =
                () -> {
                    long[] lat = new long[2_000_000];
                    int n = 0;
                    while (running.get()) {
                        long t0 = System.nanoTime();
                        List<QueueMessage> popped =
                                queue.pop(POP_BATCH, POP_WAIT_MS, TimeUnit.MILLISECONDS);
                        long dt = System.nanoTime() - t0;
                        if (n < lat.length) lat[n++] = dt;
                        popCalls.incrementAndGet();
                        for (QueueMessage m : popped) {
                            if (seen.putIfAbsent(m.getId(), 1) != null) {
                                duplicates.incrementAndGet();
                            }
                            consumed.incrementAndGet();
                            queue.ack(m.getId());
                        }
                    }
                    latencyBuffers.add(java.util.Arrays.copyOf(lat, n));
                };

        // warmup
        for (int i = 0; i < CONSUMER_THREADS; i++) consumers.submit(consumerTask);
        sleep(WARMUP_MS);

        // reset counters + Redis stats; relaunch consumers so buffers cover only the window
        seen.clear();
        consumed.set(0);
        duplicates.set(0);
        popCalls.set(0);
        latencyBuffers.clear();
        running.set(false);
        sleep(50);
        running.set(true);
        try (Jedis admin = new Jedis(HOST, PORT)) {
            admin.configResetStat();
        }
        long measureStart = System.nanoTime();
        for (int i = 0; i < CONSUMER_THREADS; i++) consumers.submit(consumerTask);

        sleep(MEASURE_MS);
        double elapsedSec = (System.nanoTime() - measureStart) / 1e9;
        running.set(false);
        producer.shutdownNow();
        consumers.shutdown();
        try {
            consumers.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        long evalsha = commandCalls("evalsha");
        long zadd = commandCalls("zadd");

        int total = latencyBuffers.stream().mapToInt(a -> a.length).sum();
        long[] all = new long[total];
        int idx = 0;
        for (long[] b : latencyBuffers) {
            System.arraycopy(b, 0, all, idx, b.length);
            idx += b.length;
        }
        java.util.Arrays.sort(all);

        long consumedN = consumed.get();
        long pops = popCalls.get();
        report.append("---- #1 ").append(name).append(" ----\n");
        report.append(
                String.format(
                        "  consumer threads=%d  refill pool=2  duration=%.1fs%n",
                        CONSUMER_THREADS, elapsedSec));
        report.append(
                String.format(
                        "  consumed:      %,d msgs  (%,.0f msgs/sec)%n",
                        consumedN, consumedN / elapsedSec));
        report.append(String.format("  pop() calls:   %,d%n", pops));
        report.append(
                String.format(
                        "  duplicates:    %,d  (%.3f%% of consumed)%n",
                        duplicates.get(), 100.0 * duplicates.get() / Math.max(1, consumedN)));
        report.append(
                String.format(
                        "  Redis evalsha: %,d  (%.3f per pop call)%n",
                        evalsha, evalsha / (double) Math.max(1, pops)));
        report.append(String.format("  Redis zadd:    %,d%n", zadd));
        report.append("  pop latency (us): ")
                .append(
                        String.format(
                                "p50=%.1f  p95=%.1f  p99=%.1f  max=%.1f%n",
                                pct(all, 50) / 1000.0,
                                pct(all, 95) / 1000.0,
                                pct(all, 99) / 1000.0,
                                (all.length == 0 ? 0 : all[all.length - 1]) / 1000.0));
        report.append("\n");

        queue.flush();
        refillPool.shutdownNow();
        pooled.close();
    }

    private long pct(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, i))];
    }

    private long commandCalls(String cmd) {
        try (Jedis admin = new Jedis(HOST, PORT)) {
            String info = admin.info("commandstats");
            for (String line : info.split("\\r?\\n")) {
                if (line.startsWith("cmdstat_" + cmd + ":")) {
                    for (String part : line.substring(line.indexOf(':') + 1).split(",")) {
                        if (part.startsWith("calls=")) {
                            return Long.parseLong(part.substring("calls=".length()));
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return 0;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void assumeRedisReachable() {
        try (Jedis j = new Jedis(HOST, PORT)) {
            Assumptions.assumeTrue("PONG".equalsIgnoreCase(j.ping()), "redis not reachable");
        } catch (Exception e) {
            Assumptions.abort(
                    "redis not reachable at " + HOST + ":" + PORT + " — " + e.getMessage());
        }
    }
}
