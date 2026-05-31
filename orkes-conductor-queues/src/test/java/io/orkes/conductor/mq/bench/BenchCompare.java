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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

/**
 * Portable, public-API-only fan-out benchmark for an apples-to-apples comparison of this branch
 * against the pre-optimization {@code main}. It uses ONLY {@link ConductorQueue#push}/{@link
 * ConductorQueue#pop}/{@link ConductorQueue#ack} and the 3-arg {@link ConductorRedisQueue}
 * constructor — all present on both — so the identical source compiles and runs on either codebase.
 * Redis op counts are read from {@code INFO commandstats} (reset at the start of the measurement
 * window), so they are codebase-agnostic. Producer and consumer share this JVM.
 *
 * <p>Skipped unless {@code -Dbench.cmp=true}. Knobs (system properties, all optional): {@code
 * bench.cmp.queues} (100), {@code .workersPerQueue} (1), {@code .batch} (10), {@code .wait} ms
 * (100), {@code .ratePerQueue} msgs/sec/queue (10, or {@code 0} to saturate), {@code .publishers}
 * (4), {@code .pool} jedis maxTotal (512), {@code .pollerThreads} (64), {@code .warmupMs} (4000),
 * {@code .measureMs} (10000), {@code .backlogCap} max outstanding in saturate mode (20000).
 *
 * <p>Reports throughput, % empty pops, duplicates, Redis ops (total + evalsha/zadd/zrem and
 * ops-per-message), and queue-wait latency percentiles. To compare, run this same {@code --tests
 * '*BenchCompare'} target from a checkout of {@code main} and from this branch with identical
 * knobs.
 */
public class BenchCompare {

    private static final String HOST = System.getProperty("bench.redis.host", "localhost");
    private static final int PORT = Integer.getInteger("bench.redis.port", 6399);

    @Test
    public void compare() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("bench.cmp"), "set -Dbench.cmp=true");
        try (Jedis j = new Jedis(HOST, PORT)) {
            Assumptions.assumeTrue("PONG".equalsIgnoreCase(j.ping()), "redis not reachable");
        } catch (Exception e) {
            Assumptions.abort("redis not reachable: " + e.getMessage());
        }

        int queues = Integer.getInteger("bench.cmp.queues", 100);
        int workersPerQueue = Integer.getInteger("bench.cmp.workersPerQueue", 1);
        int batch = Integer.getInteger("bench.cmp.batch", 10);
        int waitMs = Integer.getInteger("bench.cmp.wait", 100);
        int ratePerQueue = Integer.getInteger("bench.cmp.ratePerQueue", 10); // 0 => saturate
        int publishers = Integer.getInteger("bench.cmp.publishers", 4);
        int poolSize = Integer.getInteger("bench.cmp.pool", 512);
        int pollerThreads = Integer.getInteger("bench.cmp.pollerThreads", 64);
        long warmupMs = Integer.getInteger("bench.cmp.warmupMs", 4_000);
        long measureMs = Integer.getInteger("bench.cmp.measureMs", 10_000);
        long backlogCap = Integer.getInteger("bench.cmp.backlogCap", 20_000); // saturate mode
        String run = UUID.randomUUID().toString().substring(0, 8);

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(poolSize);
        poolConfig.setMaxIdle(poolSize);
        poolConfig.setMinIdle(Math.min(32, poolSize));
        JedisPooled pooled = new JedisPooled(poolConfig, HOST, PORT);
        UnifiedJedisCommands cmds = new UnifiedJedisCommands(pooled);
        ExecutorService pollerExec = Executors.newFixedThreadPool(pollerThreads, daemon("poller"));

        List<ConductorRedisQueue> qs = new ArrayList<>(queues);
        for (int i = 0; i < queues; i++) {
            qs.add(new ConductorRedisQueue("cmp_" + run + "_" + i, cmds, pollerExec));
        }

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong produced = new AtomicLong();
        AtomicLong consumed = new AtomicLong();
        AtomicLong popCalls = new AtomicLong();
        AtomicLong emptyPops = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
        List<long[]> latencyBuffers = new CopyOnWriteArrayList<>();

        boolean saturate = ratePerQueue <= 0;
        double totalRate = (double) queues * ratePerQueue;

        // ---- producers ----
        ExecutorService producerExec = Executors.newFixedThreadPool(publishers, daemon("producer"));
        for (int p = 0; p < publishers; p++) {
            final int pid = p;
            producerExec.submit(
                    () -> {
                        long start = System.nanoTime();
                        long sent = 0;
                        int qi = pid;
                        double ratePerPublisher = totalRate / publishers;
                        while (running.get()) {
                            if (saturate) {
                                // Keep a bounded backlog so consumers stay busy without unbounded
                                // memory growth.
                                if (produced.get() - consumed.get() > backlogCap) {
                                    LockSupport.parkNanos(200_000);
                                    continue;
                                }
                            } else {
                                // Token-bucket pacing to the offered rate.
                                double expected =
                                        (System.nanoTime() - start) / 1e9 * ratePerPublisher;
                                if (sent >= expected) {
                                    LockSupport.parkNanos(200_000);
                                    continue;
                                }
                            }
                            ConductorRedisQueue q = qs.get(qi % queues);
                            qi += publishers;
                            // id = <pushEpochMillis>:<uuid> so the consumer can compute wall-clock
                            // queue-wait latency.
                            String id = System.currentTimeMillis() + ":" + UUID.randomUUID();
                            q.push(List.of(new QueueMessage(id, "")));
                            sent++;
                            produced.incrementAndGet();
                        }
                    });
        }

        // ---- consumers ----
        int totalWorkers = queues * workersPerQueue;
        ExecutorService workers = Executors.newFixedThreadPool(totalWorkers, daemon("worker"));
        for (int i = 0; i < totalWorkers; i++) {
            final ConductorRedisQueue q = qs.get(i % queues);
            workers.submit(
                    () -> {
                        long[] lat = new long[2_000_000];
                        int n = 0;
                        while (running.get()) {
                            List<QueueMessage> popped = q.pop(batch, waitMs, TimeUnit.MILLISECONDS);
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
                                        lat[n++] = nowMs - Long.parseLong(id.substring(0, c));
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
                        latencyBuffers.add(Arrays.copyOf(lat, n));
                    });
        }

        // ---- warmup, then measure a clean window ----
        Thread.sleep(warmupMs);
        Jedis admin = new Jedis(HOST, PORT);
        admin.configResetStat();
        long c0 = consumed.get();
        long p0 = produced.get();
        long t0 = System.nanoTime();
        Thread.sleep(measureMs);
        long elapsedNs = System.nanoTime() - t0;
        long consumedDelta = consumed.get() - c0;
        long producedDelta = produced.get() - p0;
        String cmdstats = admin.info("commandstats");
        String stats = admin.info("stats");
        admin.close();

        running.set(false);
        producerExec.shutdownNow();
        workers.shutdownNow();
        workers.awaitTermination(5, TimeUnit.SECONDS);
        pollerExec.shutdownNow();

        double elapsedSec = elapsedNs / 1e9;

        int total = latencyBuffers.stream().mapToInt(a -> a.length).sum();
        long[] all = new long[total];
        int idx = 0;
        for (long[] a : latencyBuffers) {
            System.arraycopy(a, 0, all, idx, a.length);
            idx += a.length;
        }
        Arrays.sort(all);

        long totalCmds = parseInfo(stats, "total_commands_processed:");
        long evalsha = parseCmdCalls(cmdstats, "evalsha");
        long zadd = parseCmdCalls(cmdstats, "zadd");
        long zrem = parseCmdCalls(cmdstats, "zrem");
        long zrangebyscore = parseCmdCalls(cmdstats, "zrangebyscore");

        StringBuilder r = new StringBuilder();
        r.append("\n======= BENCH COMPARE (in-JVM, public API) =======\n");
        r.append(
                String.format(
                        "queues=%d workersPerQueue=%d batch=%d waitMs=%d ratePerQueue=%s maxCacheDepth=%s%n",
                        queues,
                        workersPerQueue,
                        batch,
                        waitMs,
                        saturate ? "SATURATE" : ratePerQueue,
                        System.getProperty("orkes.queue.maxCacheDepth", "default")));
        r.append(String.format("measure=%.1fs%n", elapsedSec));
        r.append(
                String.format(
                        "produced:    %,d  (%,.0f /sec offered)%n",
                        producedDelta, producedDelta / elapsedSec));
        r.append(
                String.format(
                        "consumed:    %,d  (%,.0f /sec throughput)%n",
                        consumedDelta, consumedDelta / elapsedSec));
        r.append(
                String.format(
                        "empty pops:  %,d  (%.1f%%)%n",
                        emptyPops.get(),
                        popCalls.get() == 0 ? 0.0 : 100.0 * emptyPops.get() / popCalls.get()));
        r.append(String.format("duplicates:  %,d%n", duplicates.get()));
        r.append(
                String.format(
                        "redis ops:   total=%,d (%,.0f /sec)  evalsha=%,d  zadd=%,d  zrem=%,d  zrangebyscore=%,d%n",
                        totalCmds, totalCmds / elapsedSec, evalsha, zadd, zrem, zrangebyscore));
        r.append(
                String.format(
                        "redis ops / msg consumed: %.2f%n",
                        consumedDelta == 0 ? 0.0 : (double) totalCmds / consumedDelta));
        if (total > 0) {
            r.append(
                    String.format(
                            "queue wait latency (ms): p50=%d  p90=%d  p95=%d  p99=%d  max=%d  (samples=%,d)%n",
                            all[(int) (total * 0.50)],
                            all[(int) (total * 0.90)],
                            all[(int) (total * 0.95)],
                            all[Math.min(total - 1, (int) (total * 0.99))],
                            all[total - 1],
                            total));
        }
        r.append("==================================================\n");

        String out = r.toString();
        System.out.println(out);
        String outFile = System.getProperty("bench.out");
        if (outFile != null) {
            Files.writeString(new File(outFile).toPath(), out);
        }
        pooled.close();
    }

    private static long parseInfo(String info, String key) {
        for (String line : info.split("\\r?\\n")) {
            if (line.startsWith(key)) {
                try {
                    return Long.parseLong(line.substring(key.length()).trim());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return 0;
    }

    /** Parses {@code cmdstat_<cmd>:calls=N,...} (case-insensitive command). */
    private static long parseCmdCalls(String cmdstats, String cmd) {
        String prefix = "cmdstat_" + cmd.toLowerCase() + ":calls=";
        for (String line : cmdstats.split("\\r?\\n")) {
            if (line.toLowerCase().startsWith(prefix)) {
                String rest = line.substring(prefix.length());
                int comma = rest.indexOf(',');
                try {
                    return Long.parseLong(comma > 0 ? rest.substring(0, comma) : rest);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return 0;
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
