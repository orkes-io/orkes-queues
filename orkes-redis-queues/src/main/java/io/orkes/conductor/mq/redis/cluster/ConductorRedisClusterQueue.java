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
package io.orkes.conductor.mq.redis.cluster;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.QueueMonitorProperties;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.ZAddParams;

@Slf4j
public class ConductorRedisClusterQueue implements ConductorQueue {

    private int queueUnackTime = 30_000;

    private final JedisCluster jedis;

    private final Clock clock;

    private final String queueName;

    private final String payloadKey;

    private static final BigDecimal HUNDRED = new BigDecimal(100);

    private final ClusteredQueueMonitor queueMonitor;

    public ConductorRedisClusterQueue(
            String queueName,
            JedisCluster jedisCluster,
            QueueMonitorProperties queueMonitorProperties) {
        this.jedis = jedisCluster;
        this.clock = Clock.systemDefaultZone();
        this.queueName = queueName;
        this.payloadKey = queueName + "_payload";
        this.queueMonitor =
                new ClusteredQueueMonitor(jedisCluster, queueName, queueMonitorProperties);

        log.info("ConductorRedisClusterQueue started serving {}", queueName);
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        List<QueueMessage> messages = queueMonitor.pop(count, waitTime, timeUnit);
        if (messages.isEmpty()) {
            return messages;
        }
        String[] messageIds = messages.stream().map(QueueMessage::getId).toArray(String[]::new);
        List<String> payloads = getPayloads(messageIds);
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).setPayload(payloads.get(i));
        }
        return messages;
    }

    @Override
    public boolean ack(String messageId) {
        Long removed = jedis.zrem(queueName, messageId);
        jedis.hdel(payloadKey, messageId);
        return removed > 0;
    }

    @Override
    public void push(List<QueueMessage> messages) {

        long now = clock.millis();
        Map<String, Double> scoreMembers = new HashMap<>(messages.size());
        Map<String, String> payloads = new HashMap<>();
        for (QueueMessage msg : messages) {
            double score = getScore(now, msg);
            String messageId = msg.getId();
            scoreMembers.put(messageId, score);
            if (StringUtils.isNotBlank(msg.getPayload())) {
                payloads.put(messageId, msg.getPayload());
            }
        }
        jedis.zadd(queueName, scoreMembers);
        if (!payloads.isEmpty()) {
            jedis.hmset(payloadKey, payloads);
        }
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
        jedis.hdel(payloadKey, messageId);
    }

    @Override
    public QueueMessage get(String messageId) {

        Double score = jedis.zscore(queueName, messageId);
        if (score == null) {
            return null;
        }
        int priority =
                new BigDecimal(score.doubleValue())
                        .remainder(BigDecimal.ONE)
                        .multiply(HUNDRED)
                        .intValue();
        String payload = jedis.hget(payloadKey, messageId);
        QueueMessage message = new QueueMessage(messageId, payload, score.longValue(), priority);
        return message;
    }

    @Override
    public void flush() {
        jedis.del(queueName);
        jedis.del(payloadKey);
    }

    @Override
    public long size() {
        return jedis.zcard(queueName);
    }

    @Override
    public int getQueueUnackTime() {
        return queueUnackTime;
    }

    @Override
    public void setQueueUnackTime(int queueUnackTime) {
        this.queueUnackTime = queueUnackTime;
    }

    @Override
    public String getShardName() {
        return null;
    }

    private List<String> getPayloads(String[] messageIds) {
        List<String> payloads = jedis.hmget(payloadKey, messageIds);
        return payloads;
    }
}
