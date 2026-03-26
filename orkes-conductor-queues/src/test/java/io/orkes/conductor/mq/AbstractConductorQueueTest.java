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
package io.orkes.conductor.mq;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Uninterruptibles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract base test for {@link ConductorQueue} implementations. Subclasses provide the concrete
 * queue backed by standalone Redis, Redis Cluster, or Redis Sentinel.
 */
public abstract class AbstractConductorQueueTest {

    /** Returns the primary queue instance under test. */
    protected abstract ConductorQueue getQueue();

    /** Creates a new queue with the given name against the same Redis backend. */
    protected abstract ConductorQueue createQueue(String queueName);

    private QueueMessage popOne() {
        List<QueueMessage> messages = getQueue().pop(1, 10, TimeUnit.MILLISECONDS);
        if (messages.isEmpty()) {
            return null;
        }
        return messages.get(0);
    }

    @Test
    public void testEmptyPoll() {
        getQueue().flush();
        for (int i = 0; i < 10; i++) {
            QueueMessage message = popOne();
            assertNull(message);
        }
    }

    @Test
    public void testGetName() {
        assertNotNull(getQueue().getName());
    }

    @Test
    public void testGetShardName() {
        // Default implementation returns null
        getQueue().getShardName();
    }

    @Test
    public void testExists() {
        getQueue().flush();
        String id = UUID.randomUUID().toString();
        QueueMessage msg = new QueueMessage(id, "Hello World-" + id);
        msg.setTimeout(100, TimeUnit.MILLISECONDS);
        getQueue().push(Arrays.asList(msg));

        assertTrue(getQueue().exists(id));
        assertFalse(getQueue().exists("nonexistent-id-" + UUID.randomUUID()));
    }

    @Test
    public void testGetNonExistentMessage() {
        getQueue().flush();
        QueueMessage found = getQueue().get("nonexistent-" + UUID.randomUUID());
        assertNull(found);
    }

    @Test
    public void testTimeoutUpdate() {
        getQueue().flush();

        String id = UUID.randomUUID().toString();
        QueueMessage msg = new QueueMessage(id, "Hello World-" + id);
        msg.setTimeout(100, TimeUnit.MILLISECONDS);
        getQueue().push(Arrays.asList(msg));

        QueueMessage popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

        popped = popOne();
        assertNotNull(popped);
        assertEquals(id, popped.getId());

        boolean updated = getQueue().setUnacktimeout(id, 500);
        assertTrue(updated);
        popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        popped = popOne();
        assertNotNull(popped);

        getQueue().ack(id);
        popped = popOne();
        assertNull(popped);

        QueueMessage found = getQueue().get(id);
        assertNull(found);
    }

    @Test
    public void testSetUnacktimeoutForNonExistentMessage() {
        getQueue().flush();
        boolean updated = getQueue().setUnacktimeout("nonexistent-" + UUID.randomUUID(), 500);
        assertFalse(updated);
    }

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {
        getQueue().flush();

        final int count = 100;
        final AtomicInteger published = new AtomicInteger(0);

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(6);
        CountDownLatch publishLatch = new CountDownLatch(1);
        Runnable publisher =
                new Runnable() {
                    @Override
                    public void run() {
                        List<QueueMessage> messages = new LinkedList<>();
                        for (int i = 0; i < 10; i++) {
                            QueueMessage msg =
                                    new QueueMessage(
                                            UUID.randomUUID().toString(), "Hello World-" + i);
                            msg.setPriority(new Random().nextInt(98));
                            messages.add(msg);
                        }
                        if (published.get() >= count) {
                            publishLatch.countDown();
                            return;
                        }

                        published.addAndGet(messages.size());
                        getQueue().push(messages);
                    }
                };

        for (int p = 0; p < 3; p++) {
            ses.scheduleWithFixedDelay(publisher, 1, 1, TimeUnit.MILLISECONDS);
        }
        publishLatch.await();
        CountDownLatch latch = new CountDownLatch(count);
        List<QueueMessage> allMsgs = new CopyOnWriteArrayList<>();
        AtomicInteger consumed = new AtomicInteger(0);
        AtomicInteger counter = new AtomicInteger(0);
        Runnable consumer =
                () -> {
                    if (consumed.get() >= count) {
                        return;
                    }
                    List<QueueMessage> popped = getQueue().pop(100, 1, TimeUnit.MILLISECONDS);
                    allMsgs.addAll(popped);
                    consumed.addAndGet(popped.size());
                    popped.stream().forEach(p -> latch.countDown());
                    counter.incrementAndGet();
                };
        for (int c = 0; c < 2; c++) {
            ses.scheduleWithFixedDelay(consumer, 1, 10, TimeUnit.MILLISECONDS);
        }
        Uninterruptibles.awaitUninterruptibly(latch);
        Set<QueueMessage> uniqueMessages = allMsgs.stream().collect(Collectors.toSet());

        assertEquals(count, allMsgs.size());
        assertEquals(count, uniqueMessages.size());

        ses.shutdownNow();
    }

    @Test
    public void testSetTimeout() {
        getQueue().flush();

        QueueMessage msg = new QueueMessage("x001yx-" + UUID.randomUUID(), "Hello World");
        msg.setPriority(3);
        msg.setTimeout(10_000);
        getQueue().push(Arrays.asList(msg));

        List<QueueMessage> popped = getQueue().pop(1, 1, TimeUnit.SECONDS);
        assertTrue(popped.isEmpty());

        boolean updated = getQueue().setUnacktimeout(msg.getId(), 0);
        assertTrue(updated);
        popped = getQueue().pop(2, 1, TimeUnit.SECONDS);
        assertEquals(1, popped.size());
    }

    @Test
    public void testPushAgain() {
        getQueue().flush();

        QueueMessage msg = new QueueMessage(UUID.randomUUID().toString(), null);
        msg.setTimeout(100);
        msg.setPriority(0);
        getQueue().push(Arrays.asList(msg));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        List<QueueMessage> popped = getQueue().pop(1, 100, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size());

        msg.setTimeout(10_000);
        getQueue().push(Arrays.asList(msg)); // push again!
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        popped = getQueue().pop(1, 100, TimeUnit.MILLISECONDS);
        assertEquals(0, popped.size()); // Nothing should come out

        msg.setTimeout(1);
        getQueue().push(Arrays.asList(msg)); // push again!
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        popped = getQueue().pop(1, 10, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size()); // Now it should come out
    }

    @Test
    public void testClearQueues() {
        getQueue().flush();
        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }

        getQueue().push(messages);
        assertEquals(count, getQueue().size());
        getQueue().flush();
        assertEquals(0, getQueue().size());
    }

    @Test
    public void testPriority() {
        getQueue().flush();
        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            int priority = new Random().nextInt(20);
            QueueMessage msg =
                    new QueueMessage("x" + UUID.randomUUID() + "-" + priority, "Hello World-" + i);
            msg.setPriority(priority);
            messages.add(msg);
        }
        getQueue().push(messages);
        assertEquals(count, getQueue().size());

        List<QueueMessage> popped = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());
    }

    @Test
    public void testDelayedPriority() {
        getQueue().flush();
        int count = 100;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            int priority = i + 1;
            QueueMessage msg =
                    new QueueMessage("x" + UUID.randomUUID() + "-" + priority, "Hello World-" + i);
            msg.setPriority(priority);
            msg.setTimeout(1000);
            messages.add(msg);
        }
        getQueue().push(messages);
        assertEquals(count, getQueue().size());

        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(1100));

        List<QueueMessage> popped = getQueue().pop(count, 1000, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());
    }

    @Test
    public void testScoreCalculation() {
        Clock clock = Clock.systemDefaultZone();
        long now = clock.millis();
        QueueMessage msg = new QueueMessage("a", null, 30, 44553333);
        double score = getQueue().getScore(now, msg);
        assertTrue(score > now);

        // Test with zero timeout and zero priority
        QueueMessage msg2 = new QueueMessage("b", null, 0, 0);
        double score2 = getQueue().getScore(now, msg2);
        assertEquals(now, score2, 1.0);

        // Test with zero timeout and non-zero priority
        QueueMessage msg3 = new QueueMessage("c", null, 0, 5);
        double score3 = getQueue().getScore(now, msg3);
        assertEquals(5.0, score3, 0.01);
    }

    @Test
    public void testAck() {
        getQueue().flush();
        getQueue().setQueueUnackTime(1000); // 1 sec
        assertEquals(1000, getQueue().getQueueUnackTime());

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        getQueue().push(messages);

        assertEquals(count, getQueue().size());
        List<QueueMessage> popped = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        // Wait for time longer than queue unack and messages should be available again!
        Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
        popped = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        // One more time, just to confirm!
        Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
        List<QueueMessage> popped2 = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped2);
        assertEquals(count, popped2.size());
        popped2.stream().forEach(msg -> getQueue().ack(msg.getId()));

        popped2 = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped2);
        assertEquals(0, popped2.size());

        // try to ack again
        for (QueueMessage message : popped) {
            assertFalse(getQueue().ack(message.getId()));
        }

        assertEquals(0, getQueue().size());

        // reset it back
        getQueue().setQueueUnackTime(30_000);
    }

    @Test
    public void testRemove() {
        getQueue().flush();

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        getQueue().push(messages);

        assertEquals(count, getQueue().size());
        List<QueueMessage> popped = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        popped.stream().forEach(msg -> getQueue().remove(msg.getId()));
        assertEquals(0, getQueue().size());
        popped = getQueue().pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());
    }

    @Test
    public void testGetMessage() {
        getQueue().flush();

        String id = UUID.randomUUID().toString();
        QueueMessage msg = new QueueMessage(id, "Hello World-" + id);
        msg.setPriority(5);
        getQueue().push(Arrays.asList(msg));

        QueueMessage found = getQueue().get(id);
        assertNotNull(found);
        assertEquals(id, found.getId());
    }

    @Test
    public void testAll() {
        getQueue().flush();
        assertEquals(0, getQueue().size());

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("" + i, "Hello World-" + i);
            msg.setPriority(count - 1);
            messages.add(msg);
        }
        getQueue().push(messages);

        long size = getQueue().size();
        assertEquals(count, size);

        List<QueueMessage> poped = getQueue().pop(count, 1, TimeUnit.SECONDS);
        assertNotNull(poped);
        assertEquals(count, poped.size());
        assertEquals(messages, poped);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        for (QueueMessage msg : messages) {
            QueueMessage found = getQueue().get(msg.getId());
            assertNotNull(found);
            assertEquals(msg.getId(), found.getId());
        }
        assertNull(getQueue().get("some fake id"));
        assertEquals(count, getQueue().size());
        List<QueueMessage> messages3 = getQueue().pop(count, 1, TimeUnit.SECONDS);
        if (messages3.size() < count) {
            List<QueueMessage> messages4 = getQueue().pop(count, 1, TimeUnit.SECONDS);
            messages3.addAll(messages4);
        }
    }

    @Test
    public void testRateLimitedQueue() {
        // This creates a queue with RATE_LIMITED_WORKFLOW in its name,
        // which exercises the non-cached (popStrict) path in QueueMonitor
        ConductorQueue rateLimitedQueue =
                createQueue("RATE_LIMITED_WORKFLOW_test_" + UUID.randomUUID());
        rateLimitedQueue.flush();

        int count = 5;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("rl" + i, "rate-limited-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        rateLimitedQueue.push(messages);
        assertEquals(count, rateLimitedQueue.size());

        List<QueueMessage> popped = rateLimitedQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        // Verify messages can be acked
        for (QueueMessage msg : popped) {
            assertTrue(rateLimitedQueue.ack(msg.getId()));
        }
        assertEquals(0, rateLimitedQueue.size());

        rateLimitedQueue.flush();
    }

    @Test
    public void testRateLimitedQueueEmpty() {
        ConductorQueue rateLimitedQueue =
                createQueue("RATE_LIMITED_WORKFLOW_empty_" + UUID.randomUUID());
        rateLimitedQueue.flush();

        List<QueueMessage> popped = rateLimitedQueue.pop(10, 10, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());

        rateLimitedQueue.flush();
    }
}
