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

import java.io.File;
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
import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

/**
 * True-floor, cross-JVM fan-out benchmark. A child JVM ({@link FanoutClusterPublisher}) produces
 * into N queues; THIS JVM runs the consumers (workersPerQueue per queue). Because producer and
 * consumer are in different JVMs, the in-process push-wake does not apply — this measures the real
 * cross-process throughput and push&rarr;poll latency of the poll-based delivery.
 *
 * <p>Latency is wall-clock ({@code currentTimeMillis}) so it is valid across the two JVMs on one
 * host. Skipped unless {@code -Dbench.cluster=true}.
 */
public class FanoutCluster {

    private static final String HOST = System.getProperty("bench.redis.host", "localhost");
    private static final int PORT = Integer.getInteger("bench.redis.port", 6399);

    @Test
    public void fanoutCluster() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("bench.cluster"), "set -Dbench.cluster=true");
        try (Jedis j = new Jedis(HOST, PORT)) {
            Assumptions.assumeTrue("PONG".equalsIgnoreCase(j.ping()), "redis not reachable");
        } catch (Exception e) {
            Assumptions.abort("redis not reachable: " + e.getMessage());
        }

        int queues = Integer.getInteger("bench.cluster.queues", 100);
        int workersPerQueue = Integer.getInteger("bench.cluster.workersPerQueue", 2);
        int totalWorkers = queues * workersPerQueue;
        int batch = Integer.getInteger("bench.cluster.batch", 10);
        int waitMs = Integer.getInteger("bench.cluster.wait", 100);
        int ratePerQueue = Integer.getInteger("bench.cluster.ratePerQueue", 10);
        int publishers = Integer.getInteger("bench.cluster.publishers", 4);
        int poolSize = Integer.getInteger("bench.cluster.pool", 512);
        int pollerThreads = Integer.getInteger("bench.cluster.pollerThreads", 64);
        long warmupMs = Integer.getInteger("bench.cluster.warmupMs", 4_000);
        long measureMs = Integer.getInteger("bench.cluster.measureMs", 10_000);
        boolean doorbell = Boolean.getBoolean("bench.cluster.doorbell");
        boolean realDoorbell = Boolean.getBoolean("bench.cluster.realDoorbell");
        int shards = Integer.getInteger("bench.cluster.shards", 0);
        int doorbellShards = shards > 0 ? shards : 8;
        String run = UUID.randomUUID().toString().substring(0, 8);

        // ---- launch the producer in a separate JVM ----
        String javaBin =
                System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String cp = System.getProperty("java.class.path");
        ProcessBuilder pb =
                new ProcessBuilder(
                        javaBin,
                        "-cp",
                        cp,
                        FanoutClusterPublisher.class.getName(),
                        HOST,
                        String.valueOf(PORT),
                        String.valueOf(queues),
                        String.valueOf(ratePerQueue),
                        run,
                        String.valueOf(publishers),
                        String.valueOf(doorbell),
                        String.valueOf(realDoorbell));
        pb.redirectOutput(new File("/tmp/fanout-cluster-publisher.log"));
        pb.redirectError(new File("/tmp/fanout-cluster-publisher.log"));
        Process producer = pb.start();

        // ---- consumer side (this JVM) ----
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(poolSize);
        poolConfig.setMaxIdle(poolSize);
        poolConfig.setMinIdle(Math.min(32, poolSize));
        JedisPooled pooled = new JedisPooled(poolConfig, HOST, PORT);
        UnifiedJedisCommands cmds = new UnifiedJedisCommands(pooled);
        ExecutorService pollerExec =
                Executors.newFixedThreadPool(pollerThreads, smallStack("poller"));

        // Consumer-side doorbell: sharded BLPOP listeners that wake the local pollers.
        RedisDoorbell consumerDoorbell =
                realDoorbell ? new RedisDoorbell(pooled, doorbellShards) : null;
        List<ConductorRedisQueue> qs = new ArrayList<>(queues);
        for (int i = 0; i < queues; i++) {
            qs.add(
                    new ConductorRedisQueue(
                            "fanc_" + run + "_" + i, cmds, pollerExec, consumerDoorbell));
        }

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong consumed = new AtomicLong();
        AtomicLong popCalls = new AtomicLong();
        AtomicLong emptyPops = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
        List<long[]> latencyBuffers = new CopyOnWriteArrayList<>();

        List<Runnable> taskList = new ArrayList<>();
        if (doorbell && shards > 0) {
            // Sharded multi-key BLPOP: `shards` listener threads (= `shards` Redis connections),
            // each
            // blocking on the doorbells of a PARTITION of the queues. Connections scale with shard
            // count, not queue/worker count.
            for (int s = 0; s < shards; s++) {
                final int shard = s;
                String[] keys =
                        java.util.stream.IntStream.range(0, queues)
                                .filter(qi -> qi % shards == shard)
                                .mapToObj(qi -> "door_" + run + "_" + qi)
                                .toArray(String[]::new);
                taskList.add(
                        () -> {
                            long[] lat = new long[1_000_000];
                            int n = 0;
                            while (running.get()) {
                                List<String> res = pooled.blpop(1, keys);
                                long nowMs = System.currentTimeMillis();
                                popCalls.incrementAndGet();
                                if (res == null || res.size() < 2) {
                                    emptyPops.incrementAndGet();
                                    continue;
                                }
                                String id = res.get(1);
                                int c = id.indexOf(':');
                                if (c > 0 && n < lat.length) {
                                    try {
                                        lat[n++] = nowMs - Long.parseLong(id.substring(0, c));
                                    } catch (NumberFormatException ignored) {
                                    }
                                }
                                if (seen.putIfAbsent(id, Boolean.TRUE) != null) {
                                    duplicates.incrementAndGet();
                                }
                                consumed.incrementAndGet();
                            }
                            latencyBuffers.add(java.util.Arrays.copyOf(lat, n));
                        });
            }
        } else {
            for (int i = 0; i < totalWorkers; i++) {
                final ConductorRedisQueue q = qs.get(i % queues);
                final String doorKey = "door_" + run + "_" + (i % queues);
                taskList.add(
                        doorbell
                                ? () -> {
                                    // Per-worker BLPOP: one blocked connection per worker.
                                    long[] lat = new long[200_000];
                                    int n = 0;
                                    while (running.get()) {
                                        List<String> res = pooled.blpop(1, doorKey);
                                        long nowMs = System.currentTimeMillis();
                                        popCalls.incrementAndGet();
                                        if (res == null || res.size() < 2) {
                                            emptyPops.incrementAndGet();
                                            continue;
                                        }
                                        String id = res.get(1);
                                        int c = id.indexOf(':');
                                        if (c > 0 && n < lat.length) {
                                            try {
                                                lat[n++] =
                                                        nowMs - Long.parseLong(id.substring(0, c));
                                            } catch (NumberFormatException ignored) {
                                            }
                                        }
                                        if (seen.putIfAbsent(id, Boolean.TRUE) != null) {
                                            duplicates.incrementAndGet();
                                        }
                                        consumed.incrementAndGet();
                                    }
                                    latencyBuffers.add(java.util.Arrays.copyOf(lat, n));
                                }
                                : () -> {
                                    long[] lat = new long[200_000];
                                    int n = 0;
                                    while (running.get()) {
                                        List<QueueMessage> popped =
                                                q.pop(batch, waitMs, TimeUnit.MILLISECONDS);
                                        long nowMs = System.currentTimeMillis();
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
                                                    lat[n++] =
                                                            nowMs
                                                                    - Long.parseLong(
                                                                            id.substring(0, c));
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
                                });
            }
        }
        Runnable[] tasks = taskList.toArray(new Runnable[0]);
        ExecutorService workers = Executors.newFixedThreadPool(tasks.length, smallStack("worker"));

        for (Runnable t : tasks) workers.submit(t);
        sleep(warmupMs);

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

        workers.shutdown();
        workers.awaitTermination(15, TimeUnit.SECONDS);
        producer.destroyForcibly();

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
        long cons = consumed.get();

        StringBuilder r = new StringBuilder();
        r.append("\n======= FAN-OUT CLUSTER (cross-JVM) =======\n");
        r.append(
                String.format(
                        "queues=%d  workersPerQueue=%d (separate JVM)  batch=%d  waitMs=%d  ratePerQueue=%d (~%,d/s)%n",
                        queues,
                        workersPerQueue,
                        batch,
                        waitMs,
                        ratePerQueue,
                        (long) queues * ratePerQueue));
        String mode =
                realDoorbell
                        ? ("REAL doorbell end-to-end ("
                                + doorbellShards
                                + " BLPOP listeners -> poller claim)")
                        : doorbell
                                ? (shards > 0
                                        ? ("BLPOP-doorbell sharded ("
                                                + shards
                                                + " conns for "
                                                + queues
                                                + " queues)")
                                        : "BLPOP-doorbell per-worker (" + totalWorkers + " conns)")
                                : "timed-poll";
        r.append(String.format("mode=%s%n", mode));
        if (consumerDoorbell != null) {
            r.append(
                    String.format(
                            "doorbell wakes delivered: %,d%n",
                            consumerDoorbell.wakesDelivered.get()));
        }
        r.append(String.format("measure=%.1fs%n", elapsedSec));
        r.append(String.format("consumed:    %,d  (%,.0f /sec)%n", cons, cons / elapsedSec));
        r.append(String.format("pop() calls: %,d%n", popCalls.get()));
        r.append(
                String.format(
                        "empty pops:  %,d  (%.1f%%)%n",
                        emptyPops.get(), 100.0 * emptyPops.get() / Math.max(1, popCalls.get())));
        r.append(String.format("duplicates:  %,d%n", duplicates.get()));
        r.append(
                String.format(
                        "Redis evalsha: %,d  (%,.0f /sec)   zadd: %,d%n",
                        evalsha, evalsha / elapsedSec, zadd));
        r.append(
                String.format(
                        "queue wait latency (ms): p50=%d  p90=%d  p95=%d  p99=%d  max=%d%n",
                        pct(all, 50),
                        pct(all, 90),
                        pct(all, 95),
                        pct(all, 99),
                        all.length == 0 ? 0 : all[all.length - 1]));
        r.append("===========================================\n");
        System.out.println(r);
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of(
                            System.getProperty("bench.out", "/tmp/fanout-cluster.txt")),
                    r.toString());
        } catch (Exception ignored) {
        }

        for (ConductorRedisQueue q : qs) {
            try {
                q.flush();
            } catch (Exception ignored) {
            }
        }
        if (consumerDoorbell != null) {
            consumerDoorbell.close();
        }
        pollerExec.shutdownNow();
        pooled.close();
    }

    private static java.util.concurrent.ThreadFactory smallStack(String prefix) {
        java.util.concurrent.atomic.AtomicLong n = new java.util.concurrent.atomic.AtomicLong();
        return runnable -> {
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
