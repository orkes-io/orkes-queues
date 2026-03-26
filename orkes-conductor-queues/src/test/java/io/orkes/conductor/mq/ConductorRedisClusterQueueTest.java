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

import io.orkes.conductor.mq.redis.cluster.ConductorRedisClusterQueue;
import io.orkes.conductor.mq.util.FixedPortContainer;

import com.google.common.util.concurrent.Uninterruptibles;
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

        jedisCluster = new JedisCluster(hostAndPorts);
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
}
