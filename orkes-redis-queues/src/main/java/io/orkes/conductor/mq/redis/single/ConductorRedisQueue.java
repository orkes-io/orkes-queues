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

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.QueueMonitor;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ZAddParams;

@Slf4j
public class ConductorRedisQueue implements ConductorQueue {

    private final JedisPoolAbstract jedisPool;

    private final Clock clock;

    private String queueName;

    private final QueueMonitor queueMonitor;

    public ConductorRedisQueue(String queueName, JedisPoolAbstract jedisPool) {
        this.jedisPool = jedisPool;
        this.clock = Clock.systemDefaultZone();
        this.queueName = queueName;
        this.queueMonitor = new RedisQueueMonitor(jedisPool, queueName);
        log.info("ConductorRedisQueue started serving {}", queueName);
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        return queueMonitor.pop(count, waitTime, timeUnit);
    }

    @Override
    public boolean ack(String messageId) {
        Long removed;
        try (Jedis jedis = jedisPool.getResource()) {
            removed = jedis.zrem(queueName, messageId);
        }
        return removed > 0;
    }

    @Override
    public void remove(String messageId) {

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zrem(queueName, messageId);
        }

        return;
    }

    @Override
    public void push(List<QueueMessage> messages) {

        long now = clock.millis();

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipe = jedis.pipelined();
            for (QueueMessage msg : messages) {
                double score = getScore(now, msg);
                String messageId = msg.getId();
                pipe.zadd(queueName, score, messageId);
            }

            pipe.sync();
            pipe.close();
        }
    }

    @Override
    public boolean setUnacktimeout(String messageId, long unackTimeout) {
        double score = clock.millis() + unackTimeout;
        try (Jedis jedis = jedisPool.getResource()) {
            ZAddParams params =
                    ZAddParams.zAddParams()
                            .xx() // only update, do NOT add
                            .ch(); // return modified elements count
            Long modified = jedis.zadd(queueName, score, messageId, params);
            return modified != null && modified > 0;
        }
    }

    @Override
    public boolean exists(String messageId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Double score = jedis.zscore(queueName, messageId);
            if (score != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public QueueMessage get(String messageId) {

        try (Jedis jedis = jedisPool.getResource()) {
            Double score = jedis.zscore(queueName, messageId);
            if (score == null) {
                return null;
            }
            int priority =
                    new BigDecimal(score).remainder(BigDecimal.ONE).multiply(HUNDRED).intValue();
            QueueMessage message = new QueueMessage(messageId, "", score.longValue(), priority);
            return message;
        }
    }

    @Override
    public void flush() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(queueName);
        }
    }

    @Override
    public long size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(queueName);
        }
    }

    @Override
    public int getQueueUnackTime() {
        return queueMonitor.getQueueUnackTime();
    }

    @Override
    public void setQueueUnackTime(int queueUnackTime) {
        queueMonitor.setQueueUnackTime(queueUnackTime);
    }

    @Override
    public String getShardName() {
        return null;
    }
}
