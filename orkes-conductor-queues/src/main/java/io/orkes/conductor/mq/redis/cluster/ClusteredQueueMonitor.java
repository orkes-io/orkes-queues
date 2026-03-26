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
package io.orkes.conductor.mq.redis.cluster;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import com.netflix.conductor.redis.jedis.JedisCommands;

import io.orkes.conductor.mq.redis.QueueMonitor;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/** Queue monitor implementation for Redis Cluster. */
@Slf4j
public class ClusteredQueueMonitor extends QueueMonitor {

    private final JedisCommands jedisCluster;

    private final String scriptSha;

    private final String queueName;

    /**
     * Creates a new clustered queue monitor.
     *
     * @param jedisCluster the Jedis commands interface for cluster
     * @param queueName the name of the queue to monitor
     * @param executorService the executor service for async polling
     */
    public ClusteredQueueMonitor(
            JedisCommands jedisCluster, String queueName, ExecutorService executorService) {
        super(queueName, executorService);
        this.queueName = queueName;
        this.jedisCluster = jedisCluster;
        this.scriptSha = loadScript();
    }

    private String loadScript() {
        try {

            InputStream stream = getClass().getResourceAsStream("/pop_batch.lua");
            byte[] script = stream.readAllBytes();
            byte[] response =
                    jedisCluster.scriptLoad(script, queueName.getBytes(StandardCharsets.UTF_8));
            String sha = new String(response);
            return sha;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected long queueSize() {
        return jedisCluster.zcard(queueName);
    }

    @Override
    protected List<String> pollMessages(double now, double maxTime, int batchSize) {
        try {

            Object popResponse =
                    jedisCluster.evalsha(
                            scriptSha,
                            Arrays.asList(queueName),
                            Arrays.asList("" + now, "" + maxTime, "" + batchSize));

            if (popResponse == null) {
                return null;
            }

            return (List<String>) popResponse;

        } catch (JedisNoScriptException jedisNoScriptException) {
            // This will happen if the redis server was restarted
            loadScript();
            return null;
        }
    }
}
