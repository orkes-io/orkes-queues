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
package io.orkes.conductor.mq.redis.single;

import java.io.InputStream;
import java.util.*;

import io.orkes.conductor.mq.redis.QueueMonitor;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.exceptions.JedisNoScriptException;

@Slf4j
public class RedisQueueMonitor extends QueueMonitor {

    private final JedisPoolAbstract jedisPool;

    private final String queueName;

    private final String scriptSha;

    public RedisQueueMonitor(JedisPoolAbstract jedisPool, String queueName) {
        super(queueName);
        this.jedisPool = jedisPool;
        this.queueName = queueName;
        this.scriptSha = loadScript();
    }

    @Override
    protected List<String> pollMessages(double now, double maxTime, int batchSize) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object popResponse =
                    jedis.evalsha(
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
        }
    }

    @Override
    protected long queueSize() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(queueName);
        }
    }

    private String loadScript() {
        try {

            InputStream stream = getClass().getResourceAsStream("/pop_batch.lua");
            byte[] script = stream.readAllBytes();
            try (Jedis jedis = jedisPool.getResource()) {
                byte[] response = jedis.scriptLoad(script);
                return new String(response);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
