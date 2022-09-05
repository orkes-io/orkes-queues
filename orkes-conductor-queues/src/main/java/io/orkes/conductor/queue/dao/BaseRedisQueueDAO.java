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
package io.orkes.conductor.queue.dao;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseRedisQueueDAO implements QueueDAO {

    private final String queueNamespace;

    private final String queueShard;

    private final ConcurrentHashMap<String, ConductorQueue> queues;

    public BaseRedisQueueDAO(
            QueueRedisProperties queueRedisProperties, ConductorProperties properties) {

        // Stack is used for the backward compatibility with the DynoQueues
        this.queueNamespace =
                queueRedisProperties.getQueueNamespacePrefix() + "." + properties.getStack();

        String az = queueRedisProperties.getAvailabilityZone();
        this.queueShard = az.substring(az.length() - 1);
        this.queues = new ConcurrentHashMap<>();
    }

    protected abstract ConductorQueue getConductorQueue(String queueKey);

    private final ConductorQueue get(String queueName) {
        // This scheme ensures full backward compatibility with existing DynoQueues as the drop in
        // replacement
        String queueKey = queueNamespace + ".QUEUE." + queueName + "." + queueShard;
        return queues.computeIfAbsent(queueName, (keyToCompute) -> getConductorQueue(queueKey));
    }

    @Override
    public final void push(String queueName, String id, long offsetTimeInSecond) {
        QueueMessage message = new QueueMessage(id, "", offsetTimeInSecond * 1000);
        get(queueName).push(Arrays.asList(message));
    }

    @Override
    public final void push(String queueName, String id, int priority, long offsetTimeInSecond) {
        QueueMessage message = new QueueMessage(id, "", offsetTimeInSecond * 1000, priority);
        get(queueName).push(Arrays.asList(message));
    }

    @Override
    public final void push(String queueName, List<Message> messages) {
        List<QueueMessage> queueMessages = new ArrayList<>();
        for (Message message : messages) {
            queueMessages.add(
                    new QueueMessage(
                            message.getId(), message.getPayload(), 0, message.getPriority()));
        }
        get(queueName).push(queueMessages);
    }

    @Override
    public final boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
        if (get(queueName).exists(id)) {
            return false;
        }
        push(queueName, id, offsetTimeInSecond);
        return true;
    }

    @Override
    public final boolean pushIfNotExists(
            String queueName, String id, int priority, long offsetTimeInSecond) {
        if (get(queueName).exists(id)) {
            return false;
        }
        push(queueName, id, priority, offsetTimeInSecond);
        return true;
    }

    @Override
    public final List<String> pop(String queueName, int count, int timeout) {
        // Keep the timeout to a minimum of 100ms
        if (timeout < 100) {
            timeout = 100;
        }
        List<QueueMessage> messages = get(queueName).pop(count, timeout, TimeUnit.MILLISECONDS);
        return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());
    }

    @Override
    public final List<Message> pollMessages(String queueName, int count, int timeout) {
        List<QueueMessage> queueMessages =
                get(queueName).pop(count, timeout, TimeUnit.MILLISECONDS);
        return queueMessages.stream()
                .map(
                        msg ->
                                new Message(
                                        msg.getId(),
                                        msg.getPayload(),
                                        msg.getId(),
                                        msg.getPriority()))
                .collect(Collectors.toList());
    }

    @Override
    public final void remove(String queueName, String messageId) {
        get(queueName).remove(messageId);
    }

    @Override
    public final int getSize(String queueName) {
        return (int) get(queueName).size();
    }

    @Override
    public final boolean ack(String queueName, String messageId) {
        get(queueName).ack(messageId);
        return false;
    }

    @Override
    public final boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        return get(queueName).setUnacktimeout(messageId, unackTimeout);
    }

    @Override
    public final void flush(String queueName) {
        get(queueName).flush();
    }

    @Override
    public final Map<String, Long> queuesDetail() {
        Map<String, Long> sizes = new HashMap<>();
        for (Map.Entry<String, ConductorQueue> entry : queues.entrySet()) {
            sizes.put(entry.getKey(), entry.getValue().size());
        }
        return sizes;
    }

    @Override
    public final Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        Map<String, Map<String, Map<String, Long>>> queueDetails = new HashMap<>();
        for (ConductorQueue conductorRedisQueue : queues.values()) {
            Map<String, Map<String, Long>> verbose = new HashMap<>();

            Map<String, Long> sizes = new HashMap<>();
            sizes.put("size", conductorRedisQueue.size());
            sizes.put("uacked", 0L); // we do not keep a separate queue
            verbose.put(conductorRedisQueue.getShardName(), sizes);
            queueDetails.put(conductorRedisQueue.getName(), verbose);
        }
        return queueDetails;
    }

    @Override
    public final boolean resetOffsetTime(String queueName, String id) {
        return get(queueName).setUnacktimeout(id, 0);
    }

    @Override
    public final boolean containsMessage(String queueName, String messageId) {
        return get(queueName).exists(messageId);
    }

    public boolean postpone(
            String queueName, String messageId, int priority, long postponeDurationInSeconds) {
        push(queueName, messageId, priority, postponeDurationInSeconds);
        return true;
    }
}
