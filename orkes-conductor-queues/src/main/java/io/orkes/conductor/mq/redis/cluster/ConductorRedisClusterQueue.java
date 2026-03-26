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

import java.math.BigDecimal;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.redis.jedis.JedisCommands;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.params.ZAddParams;

/** Conductor queue implementation backed by Redis Cluster. */
@Slf4j
public class ConductorRedisClusterQueue implements ConductorQueue {

    private final JedisCommands jedis;

    private final Clock clock;

    private final String queueName;

    private static final BigDecimal HUNDRED = new BigDecimal(100);

    private final ClusteredQueueMonitor queueMonitor;

    /**
     * Creates a new conductor Redis Cluster queue.
     *
     * @param queueName the name of the queue
     * @param jedisCommands the Jedis commands interface for cluster
     * @param executorService the executor service for async polling
     */
    public ConductorRedisClusterQueue(
            String queueName, JedisCommands jedisCommands, ExecutorService executorService) {
        this.jedis = jedisCommands;
        this.clock = Clock.systemDefaultZone();
        this.queueName = queueName;
        this.queueMonitor = new ClusteredQueueMonitor(jedisCommands, queueName, executorService);

        log.info("ConductorRedisClusterQueue started serving {}", queueName);
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

        long removed = jedis.zrem(queueName, messageId);
        return removed > 0;
    }

    @Override
    public void push(List<QueueMessage> messages) {
        long now = clock.millis();
        Map<String, Double> scores = new HashMap<>();
        for (QueueMessage msg : messages) {
            double score = getScore(now, msg);
            String messageId = msg.getId();
            scores.put(messageId, score);
        }
        jedis.zadd(queueName, scores);
    }

    @Override
    public boolean setUnacktimeout(String messageId, long unackTimeout) {
        double score = clock.millis() + unackTimeout;
        ZAddParams params =
                ZAddParams.zAddParams()
                        .xx() // only update, do NOT add
                        .ch(); // return modified elements count
        Long modified = jedis.zadd(queueName, score, messageId, params);
        return modified != null && modified > 0;
    }

    @Override
    public boolean exists(String messageId) {
        Double score = jedis.zscore(queueName, messageId);
        if (score != null) {
            return true;
        }
        return false;
    }

    @Override
    public void remove(String messageId) {
        jedis.zrem(queueName, messageId);
    }

    @Override
    public QueueMessage get(String messageId) {

        Double score = jedis.zscore(queueName, messageId);
        if (score == null) {
            return null;
        }
        int priority = new BigDecimal(score).remainder(BigDecimal.ONE).multiply(HUNDRED).intValue();
        return new QueueMessage(messageId, "", score.longValue(), priority);
    }

    @Override
    public void flush() {
        jedis.del(queueName);
    }

    @Override
    public long size() {
        return jedis.zcard(queueName);
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
