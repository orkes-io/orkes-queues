package io.orkes.conductor.mq.redis;

import com.google.common.util.concurrent.Uninterruptibles;
import io.orkes.conductor.mq.QueueMessage;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class QueueMonitor {

    private static final BigDecimal HUNDRED = new BigDecimal(100);

    private final Clock clock;

    private final LinkedBlockingQueue<QueueMessage> peekedMessages;

    private final ExecutorService executorService;

    private final AtomicInteger pollCount = new AtomicInteger(10_000);
    private final String queueName;

    private int queueUnackTime = 30_000;

    private long nextUpdate = 0;

    private long size = 0;

    private int maxPollCount = 200;

    public QueueMonitor(String queueName) {
        this.queueName = queueName;
        this.clock = Clock.systemDefaultZone();
        this.peekedMessages = new LinkedBlockingQueue<>();
        this.executorService = new ThreadPoolExecutor(1, 1,
                100L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxPollCount));
    }

    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {

        List<QueueMessage> messages = new ArrayList<>();
        pollCount.addAndGet(count);
        if (peekedMessages.isEmpty()) {
            __peekedMessages();
        } else  {
            try {
                executorService.submit(() -> __peekedMessages());
            } catch (RejectedExecutionException rejectedExecutionException) {
            }
        }

        long now = clock.millis();
        peekedMessages.drainTo(messages, count);
        if(messages.isEmpty()) {
            Uninterruptibles.sleepUninterruptibly(waitTime, timeUnit);
            peekedMessages.drainTo(messages, count - messages.size());
        }

        //Remove any expired messages...
        boolean hasExpiredMessages = messages.stream().parallel().anyMatch(msg -> now > msg.getExpiry());
        if(hasExpiredMessages) {
            peekedMessages.clear();
            pollCount.addAndGet(count);
            return new ArrayList<>();
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
                //Negative number shouldn't happen, but it can be zero and in that case we don't do anything!
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

                int priority = new BigDecimal(scoreString).remainder(BigDecimal.ONE).multiply(HUNDRED).intValue();
                QueueMessage message = new QueueMessage(id, "", timeout, priority);
                message.setExpiry(messageExpiry);
                peekedMessages.add(message);
            }
            pollCount.addAndGet(-1 * (response.size() / 2));
        } catch(Throwable t) {
            log.warn(t.getMessage(), t);
        }
    }

    private long getQueuedMessagesLen() {
        long now = clock.millis();
        if (now > nextUpdate) {
            size = queueSize();
            nextUpdate = now + 100; // Cache for 100 ms
        }
        return size;
    }
}
