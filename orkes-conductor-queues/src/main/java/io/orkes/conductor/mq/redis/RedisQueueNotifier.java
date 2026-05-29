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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;

/**
 * Cross-process push notifier for queue wakeups. When a message is pushed onto a queue, the
 * producer {@code PUBLISH}es the queue name on a shared channel; every process runs one subscriber
 * that, on receipt, wakes the local poller for that queue (if any) via {@link
 * QueueMonitor#notifyMessageReady()}. This delivers the cross-instance case (enqueue on process A,
 * dequeue on process B) in ~a pub/sub round-trip instead of waiting out the poller's idle backoff.
 *
 * <p>It is purely an optimization layered on the poller: notifications are best-effort (Redis
 * pub/sub is fire-and-forget), and a missed notification simply falls back to the poller's backoff
 * poll — a message is never dropped.
 *
 * <p>One subscriber connection/thread per process serves every queue (demultiplexed by the message
 * payload), so it scales with process count, not queue count. Note the single shared channel fans
 * each wake out to all subscribing processes; at very large scale a per-queue channel with
 * demand-driven (un)subscription would cut that fan-out, at the cost of subscription churn.
 */
@Slf4j
public class RedisQueueNotifier {

    public static final String DEFAULT_CHANNEL = "conductor.queue.wake";

    private final JedisPooled jedis;
    private final String channel;
    private final ConcurrentHashMap<String, Set<QueueMonitor>> registry = new ConcurrentHashMap<>();
    private final JedisPubSub pubSub;
    private volatile boolean running = true;

    public RedisQueueNotifier(JedisPooled jedis) {
        this(jedis, DEFAULT_CHANNEL);
    }

    public RedisQueueNotifier(JedisPooled jedis, String channel) {
        this.jedis = jedis;
        this.channel = channel;
        this.pubSub =
                new JedisPubSub() {
                    @Override
                    public void onMessage(String ch, String queueName) {
                        Set<QueueMonitor> monitors = registry.get(queueName);
                        if (monitors != null) {
                            for (QueueMonitor monitor : monitors) {
                                monitor.notifyMessageReady();
                            }
                        }
                    }
                };
        Thread t = new Thread(this::subscribeLoop, "orkes-queue-wake-subscriber");
        t.setDaemon(true);
        t.start();
    }

    private void subscribeLoop() {
        while (running) {
            try {
                // Blocks for the lifetime of the subscription (until unsubscribe or a connection
                // drop), holding one pooled connection.
                jedis.subscribe(pubSub, channel);
            } catch (Exception e) {
                if (running) {
                    log.warn("queue wake subscriber error, reconnecting: {}", e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /** Registers a local poller to be woken when {@code queueName} receives a wake notification. */
    public void register(String queueName, QueueMonitor monitor) {
        registry.computeIfAbsent(queueName, k -> ConcurrentHashMap.newKeySet()).add(monitor);
    }

    /** Removes a previously registered poller. */
    public void unregister(String queueName, QueueMonitor monitor) {
        Set<QueueMonitor> monitors = registry.get(queueName);
        if (monitors != null) {
            monitors.remove(monitor);
            if (monitors.isEmpty()) {
                registry.remove(queueName, monitors);
            }
        }
    }

    /** Publishes a wake for {@code queueName} to all subscribing processes. Best-effort. */
    public void publish(String queueName) {
        try {
            jedis.publish(channel, queueName);
        } catch (Exception e) {
            log.debug("queue wake publish failed for {}: {}", queueName, e.getMessage());
        }
    }

    public void close() {
        running = false;
        try {
            if (pubSub.isSubscribed()) {
                pubSub.unsubscribe();
            }
        } catch (Exception ignored) {
        }
    }
}
