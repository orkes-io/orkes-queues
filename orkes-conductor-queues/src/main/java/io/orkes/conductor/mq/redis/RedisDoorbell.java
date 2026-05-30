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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;

/**
 * Cross-process, event-driven queue wakeup using a Redis list "doorbell".
 *
 * <p>On push of a due message the producer rings the doorbell ({@code LPUSH door:<queue>}); a small
 * pool of {@code shards} listener threads each block on a <em>partition</em> of the registered
 * doorbells with a single multi-key {@code BLPOP}, and wake the local poller ({@link
 * QueueMonitor#notifyMessageReady()}) the instant a token arrives. Because the wait lives on Redis
 * (not a client-side timer), there is no poll-timing-miss: cross-instance delivery latency is a
 * round-trip rather than up to {@code waitTime}.
 *
 * <p>Properties versus a pub/sub notifier: this is <b>durable</b> (a token waits in the list if no
 * consumer is ready) and <b>load-balanced</b> (exactly one listener pops each token), and the
 * connection cost scales with <b>shard count, not queue count</b> (validated: a handful of
 * connections serve hundreds of queues at ~0&nbsp;ms). It is purely an optimization: the sorted set
 * remains the source of truth and the poller's timed backoff is the fallback, so a missed/dropped
 * token only delays delivery to the next scheduled poll — a message is never lost.
 *
 * <p>The doorbell is bounded to a single pending token per queue ({@code LPUSH} + {@code LTRIM 0
 * 0}), so it is a "there is work" flag, not a per-message queue; one wake triggers one claim of the
 * whole due batch.
 */
@Slf4j
public class RedisDoorbell {

    private static final String PREFIX = "conductor.queue.door.";
    private static final int BLPOP_TIMEOUT_SEC = 1;

    private final JedisPooled jedis;
    private final int shards;
    private final ConcurrentHashMap<String, QueueMonitor> registry = new ConcurrentHashMap<>();
    private final List<Set<String>> shardKeys;
    private final ExecutorService listeners;
    private volatile boolean running = true;

    /** Diagnostic: number of wake tokens received and dispatched. */
    public final java.util.concurrent.atomic.AtomicLong wakesDelivered =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * @param jedis connection source supporting blocking ops
     * @param shards number of listener threads/connections (each watches a partition of queues); 0
     *     for a publish-only doorbell (e.g. on a producer-only process)
     */
    public RedisDoorbell(JedisPooled jedis, int shards) {
        this.jedis = jedis;
        this.shards = Math.max(0, shards);
        this.shardKeys = new ArrayList<>();
        for (int i = 0; i < this.shards; i++) {
            shardKeys.add(ConcurrentHashMap.newKeySet());
        }
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

    private int shardOf(String queueName) {
        return Math.floorMod(queueName.hashCode(), shards);
    }

    private static String doorKey(String queueName) {
        return PREFIX + queueName;
    }

    /** Registers a local poller to be woken when {@code queueName}'s doorbell rings. */
    public void register(String queueName, QueueMonitor monitor) {
        registry.put(queueName, monitor);
        if (shards > 0) {
            shardKeys.get(shardOf(queueName)).add(doorKey(queueName));
        }
    }

    /** Removes a previously registered poller. */
    public void unregister(String queueName) {
        registry.remove(queueName);
        if (shards > 0) {
            shardKeys.get(shardOf(queueName)).remove(doorKey(queueName));
        }
    }

    /** Producer side: ring the doorbell for a queue (bounded to one pending token). Best-effort. */
    public void publish(String queueName) {
        try {
            String k = doorKey(queueName);
            jedis.lpush(k, "1");
            jedis.ltrim(k, 0, 0);
        } catch (Exception e) {
            log.debug("doorbell publish failed for {}: {}", queueName, e.getMessage());
        }
    }

    private void listen(int shard) {
        Set<String> keySet = shardKeys.get(shard);
        while (running) {
            try {
                if (keySet.isEmpty()) {
                    Thread.sleep(50);
                    continue;
                }
                String[] keys = keySet.toArray(new String[0]);
                List<String> res = jedis.blpop(BLPOP_TIMEOUT_SEC, keys);
                if (res == null || res.size() < 2) {
                    continue; // timeout — re-issue (also picks up newly registered queues)
                }
                String door = res.get(0);
                QueueMonitor monitor = registry.get(door.substring(PREFIX.length()));
                if (monitor != null) {
                    wakesDelivered.incrementAndGet();
                    monitor.notifyMessageReady();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (running) {
                    log.debug("doorbell listen error (shard {}): {}", shard, e.getMessage());
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
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
