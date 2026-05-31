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
package io.orkes.conductor.mq.redis;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.UnifiedJedis;

/**
 * Cross-process, event-driven queue wakeup using Redis lists as "doorbells".
 *
 * <p>On push of a due message the producer rings the doorbell — {@code LPUSH}ing the queue name as
 * a token onto a per-partition list — and a small pool of {@code shards} listener threads each
 * block on a partition's list with a single-key {@code BLPOP}. The instant a token arrives the
 * listener wakes the local poller ({@link QueueMonitor#notifyMessageReady()}). Because the wait
 * lives on Redis (not a client-side timer), there is no poll-timing-miss: cross-instance delivery
 * latency is a round-trip rather than up to {@code waitTime}.
 *
 * <p><b>Cluster-safe by construction.</b> Every Redis operation here is single-key: the ring is one
 * {@code LPUSH}/{@code LTRIM} on a partition list, and each listener {@code BLPOP}s a single
 * partition list at a time. Nothing spans two keys, so there is no {@code CROSSSLOT} hazard — the
 * doorbell works identically on standalone, Sentinel, and Cluster Redis. (The queue's own {@code
 * ZADD} on push is a separate, single-key op on the queue's slot.)
 *
 * <p>Properties versus a pub/sub notifier: this is <b>durable</b> (a token waits in the list if no
 * consumer is ready) and <b>load-balanced</b> (exactly one listener pops each token), and the
 * connection cost scales with <b>shard count, not queue count</b> (a handful of connections serve
 * hundreds of queues). It is purely an optimization: the sorted set remains the source of truth and
 * the poller's timed backoff is the fallback, so a missed/dropped token only delays delivery to the
 * next scheduled poll — a message is never lost.
 */
@Slf4j
public class RedisDoorbell {

    /**
     * Per-partition doorbell list key prefix. The partition index is hash-tagged so the key is one
     * slot.
     */
    private static final String PREFIX = "conductor.queue.door.";

    private static final int BLPOP_TIMEOUT_SEC = 1;

    /**
     * Number of doorbell partitions (= distinct shard lists in Redis). This is a <b>global
     * constant</b> that every process must agree on, because the producer rings the list a queue
     * hashes to and the consumer must {@code BLPOP} that same list. It is deliberately independent
     * of any single process's listener-thread count ({@code shards}): a producer-only process
     * (shards=0) and a consumer running 8 listener threads still map a given queue to the same
     * partition.
     *
     * <p>For the lowest latency a consumer should run one listener thread per partition (the
     * default: {@code shards == PARTITIONS}), so each thread stays continuously blocked on a single
     * partition and wakes the instant a token lands. Running fewer listeners than partitions still
     * works but each thread then round-robins its partitions one {@code BLPOP} at a time, so a
     * token can wait up to (partitions-per-thread × {@code BLPOP} timeout) before being noticed —
     * the timed-poll fallback bounds the worst case regardless.
     */
    private static final int PARTITIONS = Integer.getInteger("orkes.queue.doorbellPartitions", 8);

    /**
     * Upper bound on a partition list's length, enforced with {@code LTRIM} after each ring. A
     * token is only a "there is work on queue X" hint; under normal load the list drains
     * immediately and never approaches this. The cap only matters if listeners fall behind (e.g.
     * all paused), where it bounds memory at the cost of possibly dropping a stale hint — harmless,
     * since the poller's timed backoff still delivers those messages.
     */
    private static final int PARTITION_LIST_CAP =
            Integer.getInteger("orkes.queue.doorbellListCap", 100_000);

    private final UnifiedJedis jedis;
    private final int shards;
    private final ConcurrentHashMap<String, QueueMonitor> registry = new ConcurrentHashMap<>();
    private final ExecutorService listeners;
    private volatile boolean running = true;

    /** Diagnostic: number of wake tokens received and dispatched. */
    public final java.util.concurrent.atomic.AtomicLong wakesDelivered =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * @param jedis connection source supporting blocking ops (standalone, Sentinel, or Cluster)
     * @param shards number of listener threads/connections; 0 for a publish-only doorbell (e.g. on
     *     a producer-only process). Capped at {@link #PARTITIONS} (more threads than partitions
     *     would leave the extras with nothing to watch); each thread holds one blocking connection.
     */
    public RedisDoorbell(UnifiedJedis jedis, int shards) {
        this.jedis = jedis;
        // More listeners than partitions is wasteful (extras get no partitions), so cap there.
        this.shards = Math.min(Math.max(0, shards), PARTITIONS);
        if (this.shards > 0) {
            this.listeners =
                    Executors.newFixedThreadPool(
                            this.shards,
                            r -> {
                                Thread t = new Thread(r, "orkes-queue-doorbell");
                                t.setDaemon(true);
                                return t;
                            });
            for (int s = 0; s < this.shards; s++) {
                final int shard = s;
                listeners.submit(() -> listen(shard));
            }
        } else {
            this.listeners = null;
        }
    }

    /** Global, process-independent mapping from a queue to its doorbell partition. */
    private static int partitionOf(String queueName) {
        return Math.floorMod(queueName.hashCode(), PARTITIONS);
    }

    /**
     * Doorbell list key for a partition. The partition index is hash-tagged so the list lives in a
     * single slot — every op on it (LPUSH/LTRIM/BLPOP) is single-key and therefore cluster-safe.
     */
    private static String partitionKey(int partition) {
        return PREFIX + "{" + partition + "}";
    }

    /** Registers a local poller to be woken when {@code queueName}'s doorbell rings. */
    public void register(String queueName, QueueMonitor monitor) {
        registry.put(queueName, monitor);
    }

    /** Removes a previously registered poller. */
    public void unregister(String queueName) {
        registry.remove(queueName);
    }

    /**
     * Producer side: ring the doorbell for a queue. Pushes the queue name as a wake token onto its
     * partition list and trims the list to its cap. Single-key ops (cluster-safe). Best-effort: a
     * failed ring just means consumers fall back to their timed poll.
     */
    public void ring(String queueName) {
        String key = partitionKey(partitionOf(queueName));
        try {
            jedis.lpush(key, queueName);
            jedis.ltrim(key, 0, PARTITION_LIST_CAP - 1);
        } catch (Exception e) {
            log.debug("doorbell ring failed for {}: {}", queueName, e.getMessage());
        }
    }

    /**
     * Listener thread {@code listenerId}: watches the partitions assigned to it ({@code listenerId,
     * listenerId+shards, ...} up to {@link #PARTITIONS}), one single-key {@code BLPOP} at a time in
     * round-robin. With {@code shards >= PARTITIONS} each listener owns exactly one partition; with
     * fewer listeners each owns several. Single-key BLPOP keeps it cluster-safe.
     */
    private void listen(int listenerId) {
        int[] myPartitions =
                java.util.stream.IntStream.range(0, PARTITIONS)
                        .filter(p -> p % shards == listenerId)
                        .toArray();
        int idx = 0;
        while (running) {
            try {
                int partition = myPartitions[idx];
                idx = (idx + 1) % myPartitions.length;
                List<String> res = jedis.blpop(BLPOP_TIMEOUT_SEC, partitionKey(partition));
                if (res == null || res.size() < 2) {
                    continue; // timed out on this partition — move to the next
                }
                String queueName = res.get(1); // [key, token] — token is the queue name
                QueueMonitor monitor = registry.get(queueName);
                if (monitor != null) {
                    wakesDelivered.incrementAndGet();
                    monitor.notifyMessageReady();
                }
            } catch (Exception e) {
                if (running && !Thread.currentThread().isInterrupted()) {
                    log.debug(
                            "doorbell listen error (listener {}): {}", listenerId, e.getMessage());
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }

    public void close() {
        running = false;
        if (listeners != null) {
            listeners.shutdownNow();
        }
    }
}
