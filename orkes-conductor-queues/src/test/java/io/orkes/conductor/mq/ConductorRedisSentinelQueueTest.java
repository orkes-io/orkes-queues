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

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;
import io.orkes.conductor.mq.util.FixedPortContainer;

import com.google.common.util.concurrent.Uninterruptibles;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisSentineled;

/**
 * Integration tests for {@link ConductorRedisQueue} connected via Redis Sentinel using
 * TestContainers. Runs Redis master and Sentinel in a single container with fixed port mapping so
 * the sentinel-reported master address is reachable from the host.
 */
public class ConductorRedisSentinelQueueTest extends AbstractConductorQueueTest {

    private static final String queueName = "sentinel_test";
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

    @BeforeAll
    public static void setUp() {
        // Start a container running both Redis and Sentinel.
        // Redis listens on REDIS_PORT, Sentinel on SENTINEL_PORT.
        // Both use the same port inside the container and on the host (via fixed port mapping),
        // so the address sentinel reports for the master (127.0.0.1:REDIS_PORT) is reachable
        // from the test code running on the host.
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

        sentinelQueue =
                new ConductorRedisQueue(
                        queueName,
                        new UnifiedJedisCommands(jedisSentineled),
                        Executors.newFixedThreadPool(2));
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
                Executors.newFixedThreadPool(2));
    }
}
