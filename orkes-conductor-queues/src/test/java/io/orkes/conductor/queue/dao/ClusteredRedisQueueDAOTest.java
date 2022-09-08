package io.orkes.conductor.queue.dao;

import com.github.dockerjava.api.command.CreateContainerCmd;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.queue.Message;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import lombok.NonNull;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;

class ClusteredRedisQueueDAOTest {

    int[] ports = new int[]{7000, 7001, 7002, 7003, 7004, 7005};

    @Rule
    public static FixedPortContainer redis = new FixedPortContainer(DockerImageName.parse("orkesio/redis-cluster"));

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
        ClusteredRedisQueueDAO clusteredRedisQueue = new ClusteredRedisQueueDAO(registry, jedisCluster, queueRedisProperties, properties);
        ConductorQueue testQueue = clusteredRedisQueue.getConductorQueue("test_queue");
        assertNotNull(testQueue);

        testQueue.push(Arrays.asList(new QueueMessage("id1",null)));
        long size = testQueue.size();
        assertEquals(1, size);


    }
}