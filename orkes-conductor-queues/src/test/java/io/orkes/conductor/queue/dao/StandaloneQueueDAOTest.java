/*
 * Copyright 2022 Orkes, Inc.
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
package io.orkes.conductor.queue.dao;

import java.util.*;
import java.util.concurrent.*;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.netflix.conductor.core.config.ConductorProperties;

import io.orkes.conductor.queue.config.QueueRedisProperties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static org.junit.Assert.*;

public class StandaloneQueueDAOTest extends BaseQueueDAOTest {

    private static GenericContainer redis =
            new GenericContainer(DockerImageName.parse("redis:6.2.6-alpine"))
                    .withExposedPorts(6379);

    private static JedisPool jedisPool;

    @BeforeAll
    public static void setUp() {

        redis.start();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(2);
        config.setMaxTotal(10);

        jedisPool = new JedisPool(config, redis.getHost(), redis.getFirstMappedPort());
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ConductorProperties conductorProperties = new ConductorProperties();
        QueueRedisProperties queueRedisProperties = new QueueRedisProperties(conductorProperties);
        redisQueue =
                new RedisQueueDAO(
                        meterRegistry, jedisPool, queueRedisProperties, conductorProperties);
    }
}
