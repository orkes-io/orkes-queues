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
package io.orkes.conductor.queue.dao.benchmark;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.queue.Message;

import io.orkes.conductor.queue.config.QueueRedisProperties;
import io.orkes.conductor.queue.dao.BaseRedisQueueDAO;
import io.orkes.conductor.queue.dao.ClusteredRedisQueueDAO;
import io.orkes.conductor.queue.dao.RedisQueueDAO;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisQueueDAOBenchmark {

    private final BaseRedisQueueDAO queueDAO;

    private int pushCount = 100;
    private int batchSize = 10;
    private int pollThreadCount = 50;
    private int queuesCount = 100;

    public RedisQueueDAOBenchmark(BaseRedisQueueDAO queueDAO) {
        this.queueDAO = queueDAO;
    }

    private void pushALot(String queueName, int count, int batchSize) {
        long s = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            List<Message> messages = new ArrayList<>(batchSize);
            for (int k = 0; k < batchSize; k++) {
                messages.add(new Message(UUID.randomUUID().toString(), generatePayload(), null));
            }
            queueDAO.push(queueName, messages);
        }
        System.out.println(
                "Took "
                        + (System.currentTimeMillis() - s)
                        + " ms to publish "
                        + (count * batchSize)
                        + " messages");
    }

    private void runBenchmark() {
        List<String> queues = new ArrayList<>();
        for (int i = 0; i < queuesCount; i++) {
            queues.add("queue_" + i);
        }

        Map<String, CountDownLatch> latches = new HashMap<>();
        for (String queue : queues) {
            pushALot(queue, pushCount, batchSize);
            CountDownLatch latch = new CountDownLatch(pollThreadCount);
            latches.put(queue, latch);
        }

        ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<String>();
        ExecutorService es = Executors.newFixedThreadPool(pollThreadCount * queues.size());

        AtomicInteger count = new AtomicInteger(0);

        long s = System.currentTimeMillis();
        System.out.println("\nStarting to poll");

        for (String queueName : queues) {
            for (int i = 0; i < pollThreadCount; i++) {
                es.submit(
                        () -> {
                            while (true) {
                                List<String> polledIds = queueDAO.pop(queueName, batchSize, 1);
                                if (!polledIds.isEmpty()) {
                                    ids.addAll(polledIds);
                                    polledIds.stream().forEach(id -> queueDAO.ack(queueName, id));
                                    count.addAndGet(polledIds.size());
                                }

                                if (queueDAO.getSize(queueName) == 0) {
                                    latches.get(queueName).countDown();
                                    break;
                                }
                            }
                        });
            }
        }

        latches.values()
                .forEach(
                        latch -> {
                            try {
                                latch.await(1, TimeUnit.MINUTES);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
        long e = System.currentTimeMillis();
        long time = e - s;
        double avg = ((double) ids.size() / (double) time) * 1000.0;
        System.out.println(
                "\n\nI polled & ack'ed "
                        + ids.size()
                        + " == "
                        + count.get()
                        + " in "
                        + (time/1000)
                        + " second(s). Rate -> "
                        + (int) avg
                        + "/sec");
        es.shutdown();
    }

    private static void benchmarkRedisCluster() {
        MeterRegistry registry = new SimpleMeterRegistry();
        int[] ports = new int[] {7000, 7001, 7002, 7003, 7004, 7005};
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (int port : ports) {
            hostAndPorts.add(new HostAndPort("localhost", port));
        }
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMinIdle(20);
        poolConfig.setMaxIdle(20);

        JedisCluster jedisCluster = new JedisCluster(hostAndPorts);
        ConductorProperties properties = new ConductorProperties();
        QueueRedisProperties queueRedisProperties = new QueueRedisProperties(properties);
        ClusteredRedisQueueDAO clusteredRedisQueue =
                new ClusteredRedisQueueDAO(
                        registry, jedisCluster, queueRedisProperties, properties);

        RedisQueueDAOBenchmark benchmark = new RedisQueueDAOBenchmark(clusteredRedisQueue);
        benchmark.runBenchmark();

        System.out.println("Done");
        System.exit(0);
    }

    private static void benchmarkRedisStandalone() {

        MeterRegistry registry = new SimpleMeterRegistry();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(20);
        config.setMaxTotal(20);
        JedisPool jedisPool = new JedisPool(config, "localhost", 6379);
        ConductorProperties properties = new ConductorProperties();

        RedisQueueDAOBenchmark benchmark =
                new RedisQueueDAOBenchmark(
                        new RedisQueueDAO(
                                registry,
                                jedisPool,
                                new QueueRedisProperties(properties),
                                properties));

        benchmark.runBenchmark();

        System.out.println("Done");
        System.exit(0);
    }

    private static String generatePayload() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        benchmarkRedisStandalone();
    }
}
