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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

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
 * Fan-out benchmark: many queues, one dedicated worker per queue (the canonical Conductor
 * topology). Uses ONLY the stable public queue API ({@code push}/{@code pop}/{@code ack}/{@code
 * flush}) plus Redis {@code INFO commandstats}, so the identical file compiles and runs against
 * both the current branch (decoupled poller) and the original pre-optimization commit — enabling a
 * true before/after.
 *
 * <p>Skipped unless {@code -Dbench.fanout=true}. Knobs (system properties, all optional): {@code
 * bench.fanout.queues} (1000), {@code .batch} (10), {@code .wait} ms (100), {@code .ratePerQueue}
 * msgs/sec/queue (10), {@code .publishers} (4), {@code .pool} jedis maxTotal (512), {@code
 * .pollerThreads} executor size (64), {@code .warmupMs} (3000), {@code .measureMs} (10000).
 */
public class FanoutBench {

    private static final String HOST = System.getProperty("bench.redis.host", "localhost");
    private static final int PORT = Integer.getInteger("bench.redis.port", 6399);

    @Test
    public void fanout() {
        Assumptions.assumeTrue(
                Boolean.getBoolean("bench.fanout"), "set -Dbench.fanout=true to run");
        try (Jedis j = new Jedis(HOST, PORT)) {
            Assumptions.assumeTrue("PONG".equalsIgnoreCase(j.ping()), "redis not reachable");
        } catch (Exception e) {
            Assumptions.abort("redis not reachable: " + e.getMessage());
        }

        final int numQueues = Integer.getInteger("bench.fanout.queues", 1000);
        final int batch = Integer.getInteger("bench.fanout.batch", 10);
        final int waitMs = Integer.getInteger("bench.fanout.wait", 100);
        final int ratePerQueue = Integer.getInteger("bench.fanout.ratePerQueue", 10);
        final int publishers = Integer.getInteger("bench.fanout.publishers", 4);
        final int poolSize = Integer.getInteger("bench.fanout.pool", 512);
        final int pollerThreads = Integer.getInteger("bench.fanout.pollerThreads", 64);
        final long warmupMs = Integer.getInteger("bench.fanout.warmupMs", 3_000);
        final long measureMs = Integer.getInteger("bench.fanout.measureMs", 10_000);
        final long totalRate = (long) numQueues * ratePerQueue;

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(poolSize);
        poolConfig.setMaxIdle(poolSize);
        poolConfig.setMinIdle(Math.min(32, poolSize));
        JedisPooled pooled = new JedisPooled(poolConfig, HOST, PORT);
        UnifiedJedisCommands cmds = new UnifiedJedisCommands(pooled);

        // Shared poller executor for ALL queues (so Redis poll concurrency is bounded by this pool,
        // not by the worker count). Same executor is handed to every queue in both before/after.
        ExecutorService pollerExec =
                Executors.newFixedThreadPool(pollerThreads, smallStack("poller"));

        String run = UUID.randomUUID().toString().substring(0, 8);
        List<ConductorRedisQueue> queues = new ArrayList<>(numQueues);
        for (int i = 0; i < numQueues; i++) {
            ConductorRedisQueue q =
                    new ConductorRedisQueue("fan_" + run + "_" + i, cmds, pollerExec);
            q.flush();
            queues.add(q);
        }

        ExecutorService workers = Executors.newFixedThreadPool(numQueues, smallStack("worker"));
        ExecutorService pubExec = Executors.newFixedThreadPool(publishers, smallStack("pub"));
        AtomicBoolean running = new AtomicBoolean(true);

        AtomicLong published = new AtomicLong();
        AtomicLong consumed = new AtomicLong();
        AtomicLong popCalls = new AtomicLong();
        AtomicLong emptyPops = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
        List<long[]> latencyBuffers = new CopyOnWriteArrayList<>();
        AtomicInteger rrPub = new AtomicInteger();

        // ---- publishers: round-robin across queues, elapsed-time paced to totalRate ----
        long ratePerPub = Math.max(1, totalRate / publishers);
        for (int p = 0; p < publishers; p++) {
            pubExec.submit(
                    () -> {
                        long start = System.nanoTime();
                        long mine = 0;
                        while (running.get()) {
                            double elapsed = (System.nanoTime() - start) / 1e9;
                            long target = (long) (ratePerPub * elapsed);
                            if (mine < target) {
                                int qi = Math.floorMod(rrPub.getAndIncrement(), numQueues);
                                String id = System.nanoTime() + ":" + UUID.randomUUID();
                                try {
                                    queues.get(qi).push(List.of(new QueueMessage(id, "")));
                                    published.incrementAndGet();
                                } catch (Exception ignored) {
                                }
                                mine++;
                            } else {
                                LockSupport.parkNanos(50_000);
                            }
                        }
                    });
        }

        // ---- workers: one per queue, each long-polls its own queue ----
        Runnable[] workerTasks = new Runnable[numQueues];
        for (int i = 0; i < numQueues; i++) {
            final ConductorRedisQueue q = queues.get(i);
            workerTasks[i] =
                    () -> {
                        long[] lat = new long[20_000];
                        int n = 0;
                        while (running.get()) {
                            List<QueueMessage> popped = q.pop(batch, waitMs, TimeUnit.MILLISECONDS);
                            long now = System.nanoTime();
                            popCalls.incrementAndGet();
                            if (popped.isEmpty()) {
                                emptyPops.incrementAndGet();
                                continue;
                            }
                            for (QueueMessage m : popped) {
                                String id = m.getId();
                                int c = id.indexOf(':');
                                if (c > 0 && n < lat.length) {
                                    try {
                                        lat[n++] = now - Long.parseLong(id.substring(0, c));
                                    } catch (NumberFormatException ignored) {
                                    }
                                }
                                if (seen.putIfAbsent(id, Boolean.TRUE) != null) {
                                    duplicates.incrementAndGet();
                                }
                                consumed.incrementAndGet();
                                q.ack(id);
                            }
                        }
                        latencyBuffers.add(java.util.Arrays.copyOf(lat, n));
                    };
        }

        // warmup
        for (Runnable t : workerTasks) workers.submit(t);
        sleep(warmupMs);

        // reset for the measurement window
        published.set(0);
        consumed.set(0);
        popCalls.set(0);
        emptyPops.set(0);
        duplicates.set(0);
        seen.clear();
        latencyBuffers.clear();
        try (Jedis admin = new Jedis(HOST, PORT)) {
            admin.configResetStat();
        }
        long t0 = System.nanoTime();
        sleep(measureMs);
        double elapsedSec = (System.nanoTime() - t0) / 1e9;
        running.set(false);

        pubExec.shutdownNow();
        workers.shutdown();
        try {
            workers.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        long evalsha = commandCalls("evalsha");
        long zadd = commandCalls("zadd");
        long zrem = commandCalls("zrem");

        int total = latencyBuffers.stream().mapToInt(a -> a.length).sum();
        long[] all = new long[total];
        int idx = 0;
        for (long[] b : latencyBuffers) {
            System.arraycopy(b, 0, all, idx, b.length);
            idx += b.length;
        }
        java.util.Arrays.sort(all);

        long cons = consumed.get();
        long pops = popCalls.get();
        StringBuilder r = new StringBuilder();
        r.append("\n============ FAN-OUT BENCHMARK ============\n");
        r.append(
                String.format(
                        "queues=%d  workers=%d (1:1)  batch=%d  waitMs=%d  ratePerQueue=%d (total ~%,d/s)%n",
                        numQueues, numQueues, batch, waitMs, ratePerQueue, totalRate));
        r.append(
                String.format(
                        "jedis pool=%d  poller threads=%d  measure=%.1fs%n",
                        poolSize, pollerThreads, elapsedSec));
        r.append(
                String.format(
                        "published:   %,d  (%,.0f /sec)%n",
                        published.get(), published.get() / elapsedSec));
        r.append(String.format("consumed:    %,d  (%,.0f /sec)%n", cons, cons / elapsedSec));
        r.append(String.format("pop() calls: %,d%n", pops));
        r.append(
                String.format(
                        "empty pops:  %,d  (%.1f%% of pop calls)%n",
                        emptyPops.get(), 100.0 * emptyPops.get() / Math.max(1, pops)));
        r.append(String.format("duplicates:  %,d%n", duplicates.get()));
        r.append(
                String.format(
                        "Redis evalsha: %,d  (%,.0f /sec, %.2f msgs/poll)%n",
                        evalsha, evalsha / elapsedSec, cons / (double) Math.max(1, evalsha)));
        r.append(String.format("Redis zadd:  %,d   zrem: %,d%n", zadd, zrem));
        r.append(
                String.format(
                        "queue wait latency (ms): p50=%.1f  p95=%.1f  p99=%.1f  max=%.1f%n",
                        pct(all, 50) / 1e6,
                        pct(all, 95) / 1e6,
                        pct(all, 99) / 1e6,
                        (all.length == 0 ? 0 : all[all.length - 1]) / 1e6));
        r.append("===========================================\n");

        System.out.println(r);
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of(System.getProperty("bench.out", "/tmp/fanout-out.txt")),
                    r.toString());
        } catch (Exception ignored) {
        }

        for (ConductorRedisQueue q : queues) {
            try {
                q.flush();
            } catch (Exception ignored) {
            }
        }
        pollerExec.shutdownNow();
        pooled.close();
    }

    private static java.util.concurrent.ThreadFactory smallStack(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return runnable -> {
            // 256 KB stacks so thousands of worker threads fit in memory.
            Thread t = new Thread(null, runnable, prefix + "-" + n.incrementAndGet(), 256 * 1024);
            t.setDaemon(true);
            return t;
        };
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

    private static long pct(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, i))];
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
