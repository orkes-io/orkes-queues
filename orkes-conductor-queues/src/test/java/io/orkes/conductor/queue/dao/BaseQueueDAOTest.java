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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;

import com.google.common.util.concurrent.Uninterruptibles;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class BaseQueueDAOTest {

    protected static final String queueName = "test";

    protected static QueueDAO redisQueue;

    private String popOne() {
        List<String> messages = redisQueue.pop(queueName, 1, 100);
        if (messages.isEmpty()) {
            return null;
        }
        return messages.get(0);
    }

    @Test
    public void testEmptyPoll() {
        redisQueue.flush(queueName);
        int count = 0;
        for (int i = 0; i < 10; i++) {
            String message = popOne();
            if (message != null) {
                count++;
            }
        }
        assertEquals(0, count);
    }

    @Test
    public void testExists() {
        redisQueue.flush(queueName);
        String id = UUID.randomUUID().toString();
        Message msg = new Message(id, null, null);
        redisQueue.push(queueName, Arrays.asList(msg));

        assertTrue(redisQueue.containsMessage(queueName, id));
    }

    @Test
    public void testPayload() {
        redisQueue.flush(queueName);
        List<Message> messages = new ArrayList<>();
        int count = 10;
        for (int i = 0; i < count; i++) {
            messages.add(
                    new Message(
                            UUID.randomUUID().toString() + "#" + i,
                            "payload_" + i,
                            "receipt_" + i));
        }
        redisQueue.push(queueName, messages);
        List<Message> popped = redisQueue.pollMessages(queueName, 10, 100);
        assertNotNull(popped);
        assertEquals(count, popped.size());
        for (int i = 0; i < popped.size(); i++) {
            String payload = popped.get(i).getPayload();
            assertNotNull(payload);
            String id = popped.get(i).getId().split("#")[1];
            assertEquals("payload_" + id, payload);
        }
    }

    @Test
    public void testTimeoutUpdate() {

        redisQueue.flush(queueName);

        String id = UUID.randomUUID().toString();
        redisQueue.push(queueName, id, 1);

        String popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(1001, TimeUnit.MILLISECONDS);

        popped = popOne();
        Assert.assertNotNull(popped);
        assertEquals(id, popped);

        boolean updated = redisQueue.setUnackTimeout(queueName, id, 500);
        assertTrue(updated);
        popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        popped = popOne();
        Assert.assertNotNull(popped);

        redisQueue.ack(queueName, id);
        popped = popOne();
        assertNull(popped);

        assertFalse(redisQueue.containsMessage(queueName, id));
    }

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {

        redisQueue.flush(queueName);

        final int count = 100;
        final AtomicInteger published = new AtomicInteger(0);

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(6);
        CountDownLatch publishLatch = new CountDownLatch(1);
        Runnable publisher =
                new Runnable() {

                    @Override
                    public void run() {
                        List<Message> messages = new LinkedList<>();
                        for (int i = 0; i < 10; i++) {
                            Message msg = new Message(UUID.randomUUID().toString(), null, null);
                            msg.setPriority(new Random().nextInt(98));
                            messages.add(msg);
                        }
                        if (published.get() >= count) {
                            publishLatch.countDown();
                            return;
                        }

                        published.addAndGet(messages.size());
                        redisQueue.push(queueName, messages);
                    }
                };

        for (int p = 0; p < 3; p++) {
            ses.scheduleWithFixedDelay(publisher, 1, 1, TimeUnit.MILLISECONDS);
        }
        publishLatch.await();
        CountDownLatch latch = new CountDownLatch(count);
        List<String> allMsgs = new CopyOnWriteArrayList<>();
        AtomicInteger consumed = new AtomicInteger(0);
        AtomicInteger counter = new AtomicInteger(0);
        Runnable consumer =
                () -> {
                    if (consumed.get() >= count) {
                        return;
                    }
                    List<String> popped = redisQueue.pop(queueName, 100, 1);
                    allMsgs.addAll(popped);
                    consumed.addAndGet(popped.size());
                    popped.stream().forEach(p -> latch.countDown());
                    counter.incrementAndGet();
                };
        for (int c = 0; c < 2; c++) {
            ses.scheduleWithFixedDelay(consumer, 1, 10, TimeUnit.MILLISECONDS);
        }
        Uninterruptibles.awaitUninterruptibly(latch);
        System.out.println(
                "Consumed: "
                        + consumed.get()
                        + ", all: "
                        + allMsgs.size()
                        + " counter: "
                        + counter.get());
        Set<String> uniqueMessages = allMsgs.stream().collect(Collectors.toSet());

        assertEquals(count, allMsgs.size());
        assertEquals(count, uniqueMessages.size());
        List<String> more = redisQueue.pop(queueName, 1, 1);
        // If we published more than we consumed since we could've published more than we consumed
        // in which case this
        // will not be empty
        if (published.get() == consumed.get()) assertEquals(0, more.size());
        else assertEquals(1, more.size());

        ses.shutdownNow();
    }

    @Test
    public void testSetTimeout() {

        redisQueue.flush(queueName);

        Message msg = new Message("x001yx", null, null);
        msg.setPriority(3);
        redisQueue.push(queueName, Arrays.asList(msg));
        redisQueue.setUnackTimeout(queueName, msg.getId(), 10_000);

        List<String> popped = redisQueue.pop(queueName, 1, 1);
        assertTrue(popped.isEmpty());

        boolean updated = redisQueue.setUnackTimeout(queueName, msg.getId(), 0);
        assertTrue(updated);
        popped = redisQueue.pop(queueName, 2, 1);
        assertEquals(1, popped.size());
    }

    @Test
    public void testPushAgain() {

        redisQueue.flush(queueName);

        String id = UUID.randomUUID().toString();
        redisQueue.push(queueName, id, 10, 1);
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        List<String> popped = redisQueue.pop(queueName, 1, 100);
        assertEquals(1, popped.size());

        redisQueue.push(queueName, id, 10, 10);
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        popped = redisQueue.pop(queueName, 1, 100);
        assertEquals(0, popped.size()); // Nothing should come out

        redisQueue.setUnackTimeout(queueName, id, 1);
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        popped = redisQueue.pop(queueName, 1, 100);
        assertEquals(1, popped.size()); // Now it should come out
    }

    @Test
    public void testClearQueues() {
        redisQueue.flush(queueName);
        int count = 10;
        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Message msg = new Message("x" + i, null, null);
            msg.setPriority(count - i);
            messages.add(msg);
        }

        redisQueue.push(queueName, messages);
        assertEquals(count, redisQueue.getSize(queueName));
        redisQueue.flush(queueName);
        assertEquals(0, redisQueue.getSize(queueName));
    }

    @Test
    public void testPriority() {
        redisQueue.flush(queueName);
        int count = 10;
        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            // int priority = new Random().nextInt(20);
            int priority = i + 1;
            Message msg =
                    new Message("x" + UUID.randomUUID().toString() + "-" + priority, null, null);
            msg.setPriority(priority);
            messages.add(msg);
        }
        redisQueue.push(queueName, messages);
        assertEquals(count, redisQueue.getSize(queueName));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        List<String> popped = redisQueue.pop(queueName, count, 100);
        assertNotNull(popped);
        assertEquals(count, popped.size());
        for (int i = 0; i < popped.size(); i++) {
            String msg = popped.get(i);
            int priority = Integer.parseInt(msg.substring(msg.lastIndexOf("-") + 1));
            System.out.println(msg + "-" + priority);
            assertEquals(i + 1, priority);
        }
    }

    @Test
    public void testRemove() {
        redisQueue.flush(queueName);

        int count = 10;
        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Message msg = new Message("x" + i, null, null);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        redisQueue.push(queueName, messages);

        assertEquals(count, redisQueue.getSize(queueName));
        List<String> popped = redisQueue.pop(queueName, count, 100);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        popped.stream().forEach(msg -> redisQueue.remove(queueName, msg));
        assertEquals(0, redisQueue.getSize(queueName));
        popped = redisQueue.pop(queueName, count, 100);
        assertNotNull(popped);
        assertEquals(0, popped.size());
    }
}
