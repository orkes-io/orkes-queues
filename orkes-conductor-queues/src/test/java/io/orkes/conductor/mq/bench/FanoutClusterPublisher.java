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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.netflix.conductor.redis.jedis.UnifiedJedisCommands;

import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;

import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPooled;

/**
 * Standalone producer process for the cross-JVM fan-out floor test ({@link FanoutCluster} launches
 * it as a child JVM). Pushes to {@code queues} queues at {@code ratePerQueue} msgs/sec/queue, each
 * message id prefixed with the push wall-clock time ({@code System.currentTimeMillis()}, comparable
 * across JVMs on one host) so the worker JVM can compute true push&rarr;poll latency. Runs until
 * the parent kills it.
 *
 * <p>args: {@code host port queues ratePerQueue runName publishers}
 */
public final class FanoutClusterPublisher {

    private FanoutClusterPublisher() {}

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int queues = Integer.parseInt(args[2]);
        int ratePerQueue = Integer.parseInt(args[3]);
        String run = args[4];
        int publishers = args.length > 5 ? Integer.parseInt(args[5]) : 4;
        long totalRate = (long) queues * ratePerQueue;

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(Math.max(32, publishers * 4));
        JedisPooled pooled = new JedisPooled(poolConfig, host, port);
        UnifiedJedisCommands cmds = new UnifiedJedisCommands(pooled);
        // Publisher never polls, so this executor is effectively unused (no poller starts without
        // demand); a tiny pool is fine.
        ExecutorService exec = Executors.newFixedThreadPool(2);

        List<ConductorRedisQueue> qs = new ArrayList<>(queues);
        for (int i = 0; i < queues; i++) {
            qs.add(new ConductorRedisQueue("fanc_" + run + "_" + i, cmds, exec));
        }

        AtomicInteger rr = new AtomicInteger();
        long ratePerPub = Math.max(1, totalRate / publishers);
        ExecutorService pub = Executors.newFixedThreadPool(publishers);
        for (int p = 0; p < publishers; p++) {
            pub.submit(
                    () -> {
                        long start = System.nanoTime();
                        long mine = 0;
                        while (!Thread.currentThread().isInterrupted()) {
                            double elapsed = (System.nanoTime() - start) / 1e9;
                            long target = (long) (ratePerPub * elapsed);
                            if (mine < target) {
                                int qi = Math.floorMod(rr.getAndIncrement(), queues);
                                String id = System.currentTimeMillis() + ":" + UUID.randomUUID();
                                try {
                                    qs.get(qi).push(List.of(new QueueMessage(id, "")));
                                } catch (Exception ignored) {
                                }
                                mine++;
                            } else {
                                LockSupport.parkNanos(50_000);
                            }
                        }
                    });
        }
        // Block until the parent destroys this process.
        Thread.currentThread().join();
    }
}
