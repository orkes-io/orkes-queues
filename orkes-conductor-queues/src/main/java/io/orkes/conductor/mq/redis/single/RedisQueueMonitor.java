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
package io.orkes.conductor.mq.redis.single;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.netflix.conductor.redis.jedis.JedisCommands;

import io.orkes.conductor.mq.redis.QueueMonitor;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/** Queue monitor implementation for standalone Redis. */
@Slf4j
public class RedisQueueMonitor extends QueueMonitor {

    private final JedisCommands jedisPool;

    private final String queueName;

    private final String scriptSha;

    /**
     * Creates a new Redis queue monitor.
     *
     * @param jedisPool the Jedis commands interface
     * @param queueName the name of the queue to monitor
     * @param executorService the executor service for async polling
     */
    public RedisQueueMonitor(
            JedisCommands jedisPool, String queueName, ExecutorService executorService) {
        super(queueName, executorService);
        this.jedisPool = jedisPool;
        this.queueName = queueName;
        this.scriptSha = loadScript();
    }

    @Override
    protected List<String> pollMessages(double now, double maxTime, int batchSize) {
        try {
            Object popResponse =
                    jedisPool.evalsha(
                            scriptSha,
                            Arrays.asList(queueName),
                            Arrays.asList("" + now, "" + maxTime, "" + batchSize));

            if (popResponse == null) {
                return null;
            }

            return (List<String>) popResponse;
        } catch (JedisNoScriptException noScriptException) {
            // This will happen if the redis server was restarted
            loadScript();
            return null;
        } catch (JedisConnectionException jedisException) {
            log.error("redis error : {}", jedisException.getMessage(), jedisException);
            return null;
        }
    }

    @Override
    protected long queueSize() {
        return jedisPool.zcard(queueName);
    }

    private String loadScript() {
        try {

            InputStream stream = getClass().getResourceAsStream("/pop_batch.lua");
            byte[] script = stream.readAllBytes();
            byte[] response = jedisPool.scriptLoad(script, "".getBytes(StandardCharsets.UTF_8));
            return new String(response);

        } catch (Exception e) {
            log.error("error loading the queue pop script: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
