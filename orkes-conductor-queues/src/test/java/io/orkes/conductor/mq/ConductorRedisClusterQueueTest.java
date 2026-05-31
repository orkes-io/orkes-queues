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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.cluster.ConductorRedisClusterQueue;
import io.orkes.conductor.mq.util.FixedPortContainer;

import com.google.common.util.concurrent.Uninterruptibles;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * Integration tests for {@link ConductorRedisClusterQueue} using a real Redis Cluster via
 * TestContainers.
 */
public class ConductorRedisClusterQueueTest extends AbstractConductorQueueTest {

    private static final String queueName = "cluster_test";

    private static final int[] ports = new int[] {7000, 7001, 7002, 7003, 7004, 7005};

    private static FixedPortContainer redis =
            new FixedPortContainer(DockerImageName.parse("orkesio/redis-cluster"));

    static {
        redis.exposePort(7000, 7000);
        redis.exposePort(7001, 7001);
        redis.exposePort(7002, 7002);
        redis.exposePort(7003, 7003);
        redis.exposePort(7004, 7004);
        redis.exposePort(7005, 7005);
    }

    private static ConductorRedisClusterQueue clusterQueue;
    private static JedisCluster jedisCluster;

    @BeforeAll
    public static void setUp() {
        redis.withStartupTimeout(Duration.ofSeconds(120)).start();
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (int port : ports) {
            hostAndPorts.add(new HostAndPort("localhost", port));
        }

        // Pool headroom: the doorbell test runs 4 listener threads each holding a blocking BLPOP
        // connection, on top of the full suite's concurrent borrowers. JedisCluster's default pool
        // (maxTotal 8) would intermittently exhaust, so size it explicitly.
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(64);
        jedisCluster = new JedisCluster(hostAndPorts, poolConfig);
        clusterQueue =
                new ConductorRedisClusterQueue(
                        queueName,
                        new UnifiedJedisCommands(jedisCluster),
                        Executors.newFixedThreadPool(2));
    }

    @Override
    protected ConductorQueue getQueue() {
        return clusterQueue;
    }

    @Override
    protected ConductorQueue createQueue(String queueName) {
        return new ConductorRedisClusterQueue(
                queueName, new UnifiedJedisCommands(jedisCluster), Executors.newFixedThreadPool(2));
    }

    /**
     * Proves the {@link RedisDoorbell} is cluster-safe: with the doorbell enabled, a push rings a
     * per-partition list (single-key {@code LPUSH}/{@code LTRIM}, hash-tagged to one slot) and the
     * listener {@code BLPOP}s a single partition list — none of which can raise {@code CROSSSLOT}
     * on a real cluster. Reuses this class's shared cluster container (a second cluster container
     * can't run: the image advertises its nodes on fixed ports 7000-7005). Pushes a batch, drains
     * it through a doorbell-wired queue, and asserts exactly-once delivery.
     */
    @org.junit.jupiter.api.Test
    public void testDoorbellOnClusterDeliversWithoutCrossslot() {
        RedisDoorbell doorbell = new RedisDoorbell(jedisCluster, 4);
        try {
            ConductorRedisClusterQueue q =
                    new ConductorRedisClusterQueue(
                            "cluster_doorbell_" + java.util.UUID.randomUUID(),
                            new UnifiedJedisCommands(jedisCluster),
                            Executors.newFixedThreadPool(2),
                            doorbell);
            q.flush();

            int count = 200;
            java.util.List<QueueMessage> messages = new java.util.ArrayList<>(count);
            java.util.Set<String> ids = new java.util.HashSet<>();
            for (int i = 0; i < count; i++) {
                String id = "cd-" + i + "-" + java.util.UUID.randomUUID();
                ids.add(id);
                messages.add(new QueueMessage(id, "", 0, 0));
            }
            q.push(messages); // rings the doorbell on the cluster — must not CROSSSLOT

            java.util.Set<String> seen = new java.util.HashSet<>();
            int iterations = 0;
            while (seen.size() < count && iterations++ < count + 100) {
                java.util.List<QueueMessage> popped = q.pop(count, 200, TimeUnit.MILLISECONDS);
                if (popped.isEmpty() && q.size() == 0) {
                    break;
                }
                java.util.List<String> ackIds = new java.util.ArrayList<>();
                for (QueueMessage m : popped) {
                    org.junit.jupiter.api.Assertions.assertTrue(
                            seen.add(m.getId()), "duplicate delivery " + m.getId());
                    ackIds.add(m.getId());
                }
                q.ackAll(ackIds);
            }

            org.junit.jupiter.api.Assertions.assertEquals(
                    count,
                    seen.size(),
                    "every message delivered exactly once via cluster doorbell");
            org.junit.jupiter.api.Assertions.assertEquals(ids, seen, "delivered ids match pushed");
            org.junit.jupiter.api.Assertions.assertEquals(0, q.size(), "queue empty after drain");
            q.flush();
        } finally {
            doorbell.close();
        }
    }
}
