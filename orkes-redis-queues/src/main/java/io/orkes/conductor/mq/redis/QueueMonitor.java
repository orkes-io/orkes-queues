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
package io.orkes.conductor.mq.redis;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.orkes.conductor.mq.QueueMessage;

import com.google.common.util.concurrent.Uninterruptibles;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class QueueMonitor {

    private static final BigDecimal HUNDRED = new BigDecimal(100);

    private final Clock clock;

    private final LinkedBlockingDeque<QueueMessage> peekedMessages;

    private final ExecutorService executorService;

    private final AtomicInteger pollCount = new AtomicInteger(0);
    private final String queueName;

    private int queueUnackTime = 30_000;

    private long nextUpdate = 0;

    private long size = 0;

    private int maxPollCount = 100;

    public QueueMonitor(String queueName) {
        this.queueName = queueName;
        this.clock = Clock.systemDefaultZone();
        this.peekedMessages = new LinkedBlockingDeque<>();
        this.executorService =
                new ThreadPoolExecutor(
                        1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(maxPollCount));
    }

    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {

        List<QueueMessage> messages = new ArrayList<>();
        int pendingCount = pollCount.addAndGet(count);
        if (peekedMessages.isEmpty()) {
            __peekedMessages();
        } else if (peekedMessages.size() < pendingCount) {
            try {
                executorService.submit(() -> __peekedMessages());
            } catch (RejectedExecutionException rejectedExecutionException) {
            }
        }

        long now = clock.millis();
        boolean waited = false;
        for (int i = 0; i < count; i++) {
            try {
                // Why not use poll with timeout?
                // poll with timeout method seem to be using spinlock that takes up more CPU
                // The sleep method below, just does Thread.wait should be more CPU friendly
                QueueMessage message = peekedMessages.poll();
                if (message == null) {
                    if (!waited) {
                        Uninterruptibles.sleepUninterruptibly(waitTime, timeUnit);
                        waited = true;
                        continue;
                    } else {
                        return messages;
                    }
                }
                if (now > message.getExpiry()) {
                    continue;
                }
                messages.add(message);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return messages;
    }

    public int getQueueUnackTime() {
        return queueUnackTime;
    }

    public void setQueueUnackTime(int queueUnackTime) {
        this.queueUnackTime = queueUnackTime;
    }

    protected abstract List<String> pollMessages(double now, double maxTime, int batchSize);

    protected abstract long queueSize();

    private synchronized void __peekedMessages() {
        try {

            int count = Math.min(maxPollCount, pollCount.get());
            if (count <= 0) {
                if (count < 0) {
                    log.warn("Negative poll count {}", pollCount.get());
                    pollCount.set(0);
                }
                // Negative number shouldn't happen, but it can be zero and in that case we don't do
                // anything!
                return;
            }
            if (getQueuedMessagesLen() == 0) {
                pollCount.set(0); // There isn't anything in the queue
                return;
            }

            log.trace("Polling {} messages from {} with size {}", count, queueName, size);

            double now = Long.valueOf(clock.millis() + 1).doubleValue();
            double maxTime = now + queueUnackTime;
            long messageExpiry = (long) now + (queueUnackTime);
            List<String> response = pollMessages(now, maxTime, count);
            if (response == null) {
                return;
            }
            for (int i = 0; i < response.size(); i += 2) {

                long timeout = 0;
                String id = response.get(i);
                String scoreString = response.get(i + 1);

                int priority =
                        new BigDecimal(scoreString)
                                .remainder(BigDecimal.ONE)
                                .multiply(HUNDRED)
                                .intValue();
                QueueMessage message = new QueueMessage(id, "", timeout, priority);
                message.setExpiry(messageExpiry);
                peekedMessages.add(message);
            }
            pollCount.addAndGet(-1 * (response.size() / 2));
        } catch (Throwable t) {
            log.warn(t.getMessage(), t);
        }
    }

    private long getQueuedMessagesLen() {
        long now = clock.millis();
        if (now > nextUpdate) {
            size = queueSize();
            nextUpdate = now + 1000; // Cache for 1000 ms
        }
        return size;
    }
}
