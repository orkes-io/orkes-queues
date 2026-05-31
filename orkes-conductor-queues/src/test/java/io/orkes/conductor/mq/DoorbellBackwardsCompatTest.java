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
package io.orkes.conductor.mq;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Backwards-compatibility test for enabling the {@link RedisDoorbell} on a cluster that already has
 * messages.
 *
 * <p><b>Phase 1 (producer = today's behavior):</b> 10 queues are populated with 100 messages each
 * (1,000 total) using the plain 3-arg {@link ConductorRedisQueue} constructor — i.e. exactly how an
 * existing Conductor server writes messages: a plain {@code ZADD}, <em>no doorbell ring</em>.
 *
 * <p><b>Phase 2 (consumer = doorbell enabled):</b> a fresh set of queue objects over the SAME queue
 * names is created with the 4-arg constructor wired to a {@link RedisDoorbell} (sharded listeners),
 * and workers poll until drained.
 *
 * <p>Asserts every one of the 1,000 pre-existing messages is delivered exactly once — no
 * duplicates, no misses — proving a doorbell-enabled consumer reads a backlog written without a
 * doorbell. Uses TestContainers, so it always runs.
 */
public class DoorbellBackwardsCompatTest {

    private static final int QUEUES = 10;
    private static final int MESSAGES_PER_QUEUE = 100;
    private static final int TOTAL = QUEUES * MESSAGES_PER_QUEUE;

    public static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:6.2.6-alpine"))
                    .withExposedPorts(6379);

    private static JedisPooled pooled;
    private static UnifiedJedisCommands cmds;

    @BeforeAll
    public static void setUp() {
        redis.start();
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(64);
        poolConfig.setMaxIdle(64);
        pooled = new JedisPooled(poolConfig, redis.getHost(), redis.getFirstMappedPort());
        cmds = new UnifiedJedisCommands(pooled);
    }

    @AfterAll
    public static void tearDown() {
        if (pooled != null) {
            pooled.close();
        }
    }

    @Test
    public void doorbellConsumerDrainsPreExistingBacklog() throws Exception {
        String run = UUID.randomUUID().toString().substring(0, 8);
        List<String> queueNames = new ArrayList<>(QUEUES);
        for (int i = 0; i < QUEUES; i++) {
            queueNames.add("bc_" + run + "_" + i);
        }

        // ---- Phase 1: produce with the PLAIN (no-doorbell) constructor, as today's servers do
        // ----
        ExecutorService producerPollerExec = Executors.newFixedThreadPool(2, daemon("prod-poller"));
        for (String name : queueNames) {
            ConductorRedisQueue plainQueue =
                    new ConductorRedisQueue(name, cmds, producerPollerExec); // 3-arg: doorbell=null
            List<QueueMessage> batch = new ArrayList<>(MESSAGES_PER_QUEUE);
            for (int m = 0; m < MESSAGES_PER_QUEUE; m++) {
                // timeout=0 → due immediately; unique id per message.
                batch.add(new QueueMessage(name + "#" + m + "#" + UUID.randomUUID(), ""));
            }
            plainQueue.push(batch);
        }
        producerPollerExec.shutdownNow();

        // Sanity: the backlog really is in Redis, written the old way.
        long stored = 0;
        for (String name : queueNames) {
            stored += pooled.zcard(name);
        }
        assertEquals(TOTAL, stored, "all messages should be stored before consuming");

        // ---- Phase 2: consume with a DOORBELL-enabled consumer over the same queue names ----
        ExecutorService consumerPollerExec =
                Executors.newFixedThreadPool(32, daemon("cons-poller"));
        RedisDoorbell doorbell = new RedisDoorbell(pooled, 4); // 4 sharded BLPOP listeners
        List<ConductorRedisQueue> consumerQueues = new ArrayList<>(QUEUES);
        for (String name : queueNames) {
            consumerQueues.add(new ConductorRedisQueue(name, cmds, consumerPollerExec, doorbell));
        }

        ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
        AtomicLong consumed = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        CountDownLatch done = new CountDownLatch(TOTAL);

        int workersPerQueue = 2;
        ExecutorService workers =
                Executors.newFixedThreadPool(QUEUES * workersPerQueue, daemon("worker"));
        for (int i = 0; i < QUEUES * workersPerQueue; i++) {
            final ConductorRedisQueue q = consumerQueues.get(i % QUEUES);
            workers.submit(
                    () -> {
                        while (consumed.get() < TOTAL && !Thread.currentThread().isInterrupted()) {
                            List<QueueMessage> popped = q.pop(10, 200, TimeUnit.MILLISECONDS);
                            for (QueueMessage msg : popped) {
                                if (seen.putIfAbsent(msg.getId(), Boolean.TRUE) != null) {
                                    duplicates.incrementAndGet();
                                } else {
                                    consumed.incrementAndGet();
                                    done.countDown();
                                }
                                q.ack(msg.getId());
                            }
                        }
                    });
        }

        boolean drained = done.await(60, TimeUnit.SECONDS);

        workers.shutdownNow();
        workers.awaitTermination(5, TimeUnit.SECONDS);
        doorbell.close();
        consumerPollerExec.shutdownNow();

        // ---- Assertions: all 1,000 delivered exactly once, none lost, none duplicated ----
        long remaining = 0;
        for (String name : queueNames) {
            remaining += pooled.zcard(name);
        }

        assertEquals(0, duplicates.get(), "no message should be delivered more than once");
        assertEquals(TOTAL, seen.size(), "every distinct message should be delivered exactly once");
        assertEquals(TOTAL, consumed.get(), "all messages should be consumed");
        assertTrue(drained, "all " + TOTAL + " messages should be drained within the timeout");
        assertEquals(0, remaining, "all messages should be acked/removed from Redis");
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
