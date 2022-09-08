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

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.ConductorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.queue.config.QueueRedisProperties;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ClusteredRedisQueueDAOTest {

    int[] ports = new int[] {7000, 7001, 7002, 7003, 7004, 7005};

    @Rule
    public static FixedPortContainer redis =
            new FixedPortContainer(DockerImageName.parse("orkesio/redis-cluster"));

    static {
        redis.exposePort(7000, 7000);
        redis.exposePort(7001, 7001);
        redis.exposePort(7002, 7002);
        redis.exposePort(7003, 7003);
        redis.exposePort(7004, 7004);
        redis.exposePort(7005, 7005);
    }

    @Test
    void getConductorQueue() {

        redis.withStartupTimeout(Duration.ofSeconds(10)).start();
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (int port : ports) {
            hostAndPorts.add(new HostAndPort("localhost", port));
        }

        JedisCluster jedisCluster = new JedisCluster(hostAndPorts);
        ConductorProperties properties = new ConductorProperties();
        QueueRedisProperties queueRedisProperties = new QueueRedisProperties(properties);
        ClusteredRedisQueueDAO clusteredRedisQueue =
                new ClusteredRedisQueueDAO(
                        registry, jedisCluster, queueRedisProperties, properties);
        ConductorQueue testQueue = clusteredRedisQueue.getConductorQueue("test_queue");
        assertNotNull(testQueue);

        testQueue.push(Arrays.asList(new QueueMessage("id1", null)));
        long size = testQueue.size();
        assertEquals(1, size);
    }
}
