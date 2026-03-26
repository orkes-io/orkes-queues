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
package io.orkes.conductor.mq.redis;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.orkes.conductor.mq.QueueMessage;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/** Abstract queue monitor that handles message polling and caching from Redis-backed queues. */
@Slf4j
public abstract class QueueMonitor {

    private static final BigDecimal HUNDRED = new BigDecimal(100);

    private final Clock clock;

    private final LinkedBlockingQueue<QueueMessage> peekedMessages;

    private ExecutorService executorService;

    private final AtomicInteger pollCount = new AtomicInteger(0);

    @Getter @Setter private int queueUnackTime = 30_000;

    private final int MAX_POLL_COUNT = 1000;

    private final boolean cached;

    private final String queueName;

    /**
     * Creates a new queue monitor.
     *
     * @param queueName the name of the queue to monitor
     * @param executorService the executor service for async polling
     */
    public QueueMonitor(String queueName, ExecutorService executorService) {
        this.clock = Clock.systemDefaultZone();
        this.peekedMessages = new LinkedBlockingQueue<>();
        this.executorService = executorService;
        this.cached = !queueName.contains("RATE_LIMITED_WORKFLOW");
        this.queueName = queueName;
    }

    /**
     * Pops messages from the queue, using caching for non-rate-limited queues.
     *
     * @param count number of messages to pop
     * @param waitTime time to wait if no messages are available
     * @param timeUnit time unit for waitTime
     * @return list of popped messages
     */
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        if (!cached) {
            return popStrict(count);
        }

        List<QueueMessage> messages = new ArrayList<>();
        int pendingCount = pollCount.addAndGet(count);
        if (peekedMessages.isEmpty()) {
            __peekedMessages();
        } else if (peekedMessages.size() < pendingCount) {
            try {
                executorService.submit(this::__peekedMessages);
            } catch (RejectedExecutionException ignored) {
            }
        }

        long now = clock.millis();
        peekedMessages.drainTo(messages, count);
        if (messages.isEmpty()) {
            try {
                QueueMessage message = peekedMessages.poll(waitTime, timeUnit);
                if (message != null && (now < message.getExpiry())) {
                    peekedMessages.add(message);
                    int remaining = count - messages.size();
                    peekedMessages.drainTo(messages, remaining);
                }
            } catch (InterruptedException ie) {
                // Ignore
            }
        }

        // Remove any expired messages...
        // The above code has the check but this is added at the end to ensure even after the wait,
        // the messages are still unexpired
        // This was added after fixing a bug, so do not remove
        boolean hasExpiredMessages =
                messages.stream()
                        .parallel() // safe
                        .anyMatch(msg -> now > msg.getExpiry());
        if (hasExpiredMessages) {
            peekedMessages.clear();
            pollCount.addAndGet(count);
            return new ArrayList<>();
        }
        return messages;
    }

    /**
     * Polls messages from the underlying store.
     *
     * @param now the current time as a double
     * @param maxTime the maximum score for messages to poll
     * @param batchSize the maximum number of messages to poll
     * @return list of alternating message ids and scores, or null if no messages
     */
    protected abstract List<String> pollMessages(double now, double maxTime, int batchSize);

    /**
     * Returns the current size of the queue.
     *
     * @return the number of messages in the queue
     */
    protected abstract long queueSize();

    private void __peekedMessages() {
        try {

            int count = Math.min(MAX_POLL_COUNT, pollCount.get());
            if (count <= 0) {
                if (count < 0) {
                    log.warn("Negative poll count {}", pollCount.get());
                    pollCount.set(0);
                }
                // Negative number shouldn't happen, but it can be zero and in that case we don't do
                // anything!
                return;
            }
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

    private List<QueueMessage> popStrict(int count) {
        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double maxTime = now + queueUnackTime;
        long messageExpiry = (long) now + (queueUnackTime);
        List<String> response = pollMessages(now, maxTime, count);
        List<QueueMessage> result = new ArrayList<>();
        if (response == null) {
            return result;
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
            result.add(message);
        }
        return result;
    }
}
