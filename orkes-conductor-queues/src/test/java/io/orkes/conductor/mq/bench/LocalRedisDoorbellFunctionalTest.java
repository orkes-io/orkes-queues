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

import java.util.concurrent.Executors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.AbstractConductorQueueTest;
import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;

/**
 * Same full correctness suite as {@link LocalRedisFunctionalTest}, but with every queue wired to a
 * {@link RedisDoorbell} so the enqueue path goes through the combined ZADD+ring Lua ({@link
 * RedisDoorbell#pushDueAndRing}). This is what validates that the script's {@code String.valueOf}
 * score encoding preserves priority/delay ordering exactly as the plain {@code ZADD} path does
 * (notably {@code testDelayedPriority}). A publish-only doorbell ({@code shards=0}) is enough — pop
 * still flows through the poller; we are exercising the push/score path, not the BLPOP wake.
 *
 * <p>Skipped unless {@code -Dbench=true} and Redis is reachable at {@code
 * bench.redis.host:bench.redis.port} (defaults {@code localhost:6399}).
 */
public class LocalRedisDoorbellFunctionalTest extends AbstractConductorQueueTest {

    private static final String HOST = System.getProperty("bench.redis.host", "localhost");
    private static final int PORT = Integer.getInteger("bench.redis.port", 6399);

    private static JedisPooled pooled;
    private static RedisDoorbell doorbell;
    private static ConductorRedisQueue redisQueue;

    @BeforeAll
    public static void setUp() {
        if (!Boolean.getBoolean("bench")) {
            return;
        }
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(2);
        poolConfig.setMaxTotal(10);
        pooled = new JedisPooled(poolConfig, HOST, PORT);
        doorbell = new RedisDoorbell(pooled, 0); // publish-only: exercises pushDueAndRing
        redisQueue =
                new ConductorRedisQueue(
                        "local_doorbell_functional_test",
                        new UnifiedJedisCommands(pooled),
                        Executors.newFixedThreadPool(2),
                        doorbell);
    }

    @BeforeEach
    public void requireRedis() {
        Assumptions.assumeTrue(Boolean.getBoolean("bench"), "set -Dbench=true to run");
        Assumptions.assumeTrue(redisQueue != null, "redis not initialized");
    }

    @Override
    protected ConductorQueue getQueue() {
        return redisQueue;
    }

    @Override
    protected ConductorQueue createQueue(String queueName) {
        return new ConductorRedisQueue(
                queueName,
                new UnifiedJedisCommands(pooled),
                Executors.newFixedThreadPool(2),
                doorbell);
    }
}
