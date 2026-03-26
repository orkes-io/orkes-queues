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
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/** Integration tests for {@link ConductorRedisQueue} using standalone Redis via TestContainers. */
public class ConductorRedisQueueTest extends AbstractConductorQueueTest {

    private static final String queueName = "standalone_test";

    public static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:6.2.6-alpine"))
                    .withExposedPorts(6379);

    private static ConductorRedisQueue redisQueue;
    private static UnifiedJedis unifiedJedis;

    @BeforeAll
    public static void setUp() {
        redis.start();

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(2);
        poolConfig.setMaxTotal(10);
        unifiedJedis = new JedisPooled(poolConfig, redis.getHost(), redis.getFirstMappedPort());

        redisQueue =
                new ConductorRedisQueue(
                        queueName,
                        new UnifiedJedisCommands(unifiedJedis),
                        Executors.newFixedThreadPool(2));
    }

    @Override
    protected ConductorQueue getQueue() {
        return redisQueue;
    }

    @Override
    protected ConductorQueue createQueue(String queueName) {
        return new ConductorRedisQueue(
                queueName, new UnifiedJedisCommands(unifiedJedis), Executors.newFixedThreadPool(2));
    }
}
