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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.RedisDoorbell;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;
import io.orkes.conductor.mq.util.FixedPortContainer;

import com.google.common.util.concurrent.Uninterruptibles;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisSentineled;

/**
 * Runs the full {@link AbstractConductorQueueTest} correctness suite against Redis via Sentinel
 * with the {@link RedisDoorbell} enabled. Proves the doorbell works on a sentinel-fronted
 * connection (a {@link JedisSentineled}, not just a {@link redis.clients.jedis.JedisPooled}): the
 * combined ZADD+ring enqueue script and the sharded {@code BLPOP} wake both run through the
 * sentinel-resolved master. Mirrors {@link ConductorRedisSentinelQueueTest}'s single-container
 * master+sentinel setup; uses TestContainers, so it always runs.
 */
public class ConductorRedisSentinelDoorbellQueueTest extends AbstractConductorQueueTest {

    private static final String queueName = "sentinel_doorbell_test";
    private static final int REDIS_PORT = 6399;
    private static final int SENTINEL_PORT = 26399;

    private static FixedPortContainer sentinelRedis =
            new FixedPortContainer(DockerImageName.parse("redis:7.0-alpine"));

    static {
        sentinelRedis.exposePort(REDIS_PORT, REDIS_PORT);
        sentinelRedis.exposePort(SENTINEL_PORT, SENTINEL_PORT);
    }

    private static ConductorRedisQueue sentinelQueue;
    private static JedisSentineled jedisSentineled;
    private static RedisDoorbell doorbell;

    @BeforeAll
    public static void setUp() {
        sentinelRedis
                .withCommand(
                        "sh",
                        "-c",
                        "redis-server --port "
                                + REDIS_PORT
                                + " --save '' --appendonly no --daemonize yes && "
                                + "printf 'port "
                                + SENTINEL_PORT
                                + "\\n"
                                + "sentinel monitor mymaster 127.0.0.1 "
                                + REDIS_PORT
                                + " 1\\n"
                                + "sentinel down-after-milliseconds mymaster 5000\\n"
                                + "sentinel failover-timeout mymaster 10000\\n' > /tmp/sentinel.conf && "
                                + "redis-sentinel /tmp/sentinel.conf")
                .withStartupTimeout(Duration.ofSeconds(60))
                .start();

        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

        HostAndPort sentinelAddr = new HostAndPort("127.0.0.1", SENTINEL_PORT);
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().build();
        Set<HostAndPort> sentinels = new HashSet<>();
        sentinels.add(sentinelAddr);
        jedisSentineled = new JedisSentineled("mymaster", clientConfig, sentinels, clientConfig);
        doorbell = new RedisDoorbell(jedisSentineled, 4);

        sentinelQueue =
                new ConductorRedisQueue(
                        queueName,
                        new UnifiedJedisCommands(jedisSentineled),
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
        return sentinelQueue;
    }

    @Override
    protected ConductorQueue createQueue(String queueName) {
        return new ConductorRedisQueue(
                queueName,
                new UnifiedJedisCommands(jedisSentineled),
                Executors.newFixedThreadPool(2),
                doorbell);
    }
}
