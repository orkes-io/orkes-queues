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

import java.util.concurrent.Executors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/**
 * Runs the full {@link AbstractConductorQueueTest} correctness suite against standalone Redis with
 * the {@link RedisDoorbell} enabled, so the doorbell enqueue path (combined ZADD+ring Lua) and the
 * sharded {@code BLPOP} wake are exercised under the same
 * priority/delay/ack/concurrency/no-duplicate assertions as the plain {@link
 * ConductorRedisQueueTest}. Uses TestContainers, so it always runs.
 */
public class ConductorRedisQueueDoorbellTest extends AbstractConductorQueueTest {

    private static final String queueName = "doorbell_test";

    public static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:6.2.6-alpine"))
                    .withExposedPorts(6379);

    private static ConductorRedisQueue redisQueue;
    private static UnifiedJedis unifiedJedis;
    private static RedisDoorbell doorbell;

    @BeforeAll
    public static void setUp() {
        redis.start();

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(2);
        // Headroom: each of the doorbell's listener threads holds one blocking BLPOP connection for
        // its full cycle, and the full suite (e.g. testConcurrency) borrows several more
        // concurrently. A tight pool would intermittently exhaust under that combination, so size
        // it
        // well above (listeners + peak concurrent borrowers).
        poolConfig.setMaxTotal(64);
        unifiedJedis = new JedisPooled(poolConfig, redis.getHost(), redis.getFirstMappedPort());
        doorbell = new RedisDoorbell(unifiedJedis, 4); // 4 sharded BLPOP listeners

        redisQueue =
                new ConductorRedisQueue(
                        queueName,
                        new UnifiedJedisCommands(unifiedJedis),
                        Executors.newFixedThreadPool(2),
                        doorbell);
    }

    @AfterAll
    public static void tearDown() {
        if (doorbell != null) {
            doorbell.close();
        }
    }

    @Override
    protected ConductorQueue getQueue() {
        return redisQueue;
    }

    @Override
    protected ConductorQueue createQueue(String queueName) {
        return new ConductorRedisQueue(
                queueName,
                new UnifiedJedisCommands(unifiedJedis),
                Executors.newFixedThreadPool(2),
                doorbell);
    }
}
