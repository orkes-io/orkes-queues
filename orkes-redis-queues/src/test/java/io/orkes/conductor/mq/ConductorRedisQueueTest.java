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
package io.orkes.conductor.mq;

import com.google.common.util.concurrent.Uninterruptibles;
import io.orkes.conductor.mq.redis.single.ConductorRedisQueue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConductorRedisQueueTest {

    private static final String redisKeyPrefix = "test_queue";

    private static final String queueName = "test";

    @Rule
    public static GenericContainer redis =
            new GenericContainer(DockerImageName.parse("redis:6.2.6-alpine"))
                    .withExposedPorts(6379);

    @Rule static ConductorRedisQueue redisQueue;

    private static JedisPool jedisPool;

    @BeforeAll
    public static void setUp() {

        redis.start();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(2);
        config.setMaxTotal(10);

        jedisPool = new JedisPool(config, redis.getHost(), redis.getFirstMappedPort());
        redisQueue = new ConductorRedisQueue(queueName, jedisPool);
    }

    private QueueMessage popOne() {
        List<QueueMessage> messages = redisQueue.pop(1, 10, TimeUnit.MILLISECONDS);
        if (messages.isEmpty()) {
            return null;
        }
        return messages.get(0);
    }

    @Test
    public void testEmptyPoll() {
        redisQueue.flush();
        ConductorRedisQueue redisQueue2 = new ConductorRedisQueue(queueName + "X", jedisPool);
        int count = 0;
        for (int i = 0; i < 10; i++) {
            QueueMessage message = popOne();
            if (message != null) {
                count++;
            }
        }
        assertEquals(0, count);
    }

    @Test
    public void testExists() {
        redisQueue.flush();
        String id = UUID.randomUUID().toString();
        QueueMessage msg = new QueueMessage(id, "Hello World-" + id);
        msg.setTimeout(100, TimeUnit.MILLISECONDS);
        redisQueue.push(Arrays.asList(msg));

        assertTrue(redisQueue.exists(id));
    }

    @Test
    public void testTimeoutUpdate() {

        redisQueue.flush();

        String id = UUID.randomUUID().toString();
        QueueMessage msg = new QueueMessage(id, "Hello World-" + id);
        msg.setTimeout(100, TimeUnit.MILLISECONDS);
        redisQueue.push(Arrays.asList(msg));

        QueueMessage popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

        popped = popOne();
        Assert.assertNotNull(popped);
        assertEquals(id, popped.getId());

        boolean updated = redisQueue.setUnacktimeout(id, 500);
        assertTrue(updated);
        popped = popOne();
        assertNull(popped);

        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        popped = popOne();
        Assert.assertNotNull(popped);

        redisQueue.ack(id);
        popped = popOne();
        assertNull(popped);

        QueueMessage found = redisQueue.get(id);
        assertNull(found);
    }

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {

        redisQueue.flush();

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
                        redisQueue.push(messages);
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
                    List<QueueMessage> popped = redisQueue.pop(100, 1, TimeUnit.MILLISECONDS);
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
        Set<QueueMessage> uniqueMessages = allMsgs.stream().collect(Collectors.toSet());

        assertEquals(count, allMsgs.size());
        assertEquals(count, uniqueMessages.size());
        List<QueueMessage> more = redisQueue.pop(1, 1, TimeUnit.SECONDS);
        // If we published more than we consumed since we could've published more than we consumed
        // in which case this
        // will not be empty
        if (published.get() == consumed.get()) assertEquals(0, more.size());
        else assertEquals(1, more.size());

        ses.shutdownNow();
    }

    @Test
    public void testSetTimeout() {

        redisQueue.flush();

        QueueMessage msg = new QueueMessage("x001yx", "Hello World");
        msg.setPriority(3);
        msg.setTimeout(10_000);
        redisQueue.push(Arrays.asList(msg));

        List<QueueMessage> popped = redisQueue.pop(1, 1, TimeUnit.SECONDS);
        assertTrue(popped.isEmpty());

        boolean updated = redisQueue.setUnacktimeout(msg.getId(), 0);
        assertTrue(updated);
        popped = redisQueue.pop(2, 1, TimeUnit.SECONDS);
        assertEquals(1, popped.size());
    }

    @Test
    public void testPushAgain() {

        redisQueue.flush();

        QueueMessage msg = new QueueMessage(UUID.randomUUID().toString(), null);
        msg.setTimeout(100);
        msg.setPriority(0);
        redisQueue.push(Arrays.asList(msg));
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        List<QueueMessage> popped = redisQueue.pop(1, 100, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size());

        msg.setTimeout(10_000);
        redisQueue.push(Arrays.asList(msg)); // push again!
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        popped = redisQueue.pop(1, 100, TimeUnit.MILLISECONDS);
        assertEquals(0, popped.size()); // Nothing should come out

        msg.setTimeout(1);
        redisQueue.push(Arrays.asList(msg)); // push again!
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        popped = redisQueue.pop(1, 10, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size()); // Now it should come out
    }

    @Test
    public void testClearQueues() {
        redisQueue.flush();
        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }

        redisQueue.push(messages);
        assertEquals(count, redisQueue.size());
        redisQueue.flush();
        assertEquals(0, redisQueue.size());
    }

    @Test
    public void testPriority() {
        redisQueue.flush();
        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            int priority = new Random().nextInt(20);
            QueueMessage msg =
                    new QueueMessage(
                            "x" + UUID.randomUUID().toString() + "-" + priority,
                            "Hello World-" + i);
            msg.setPriority(priority);
            messages.add(msg);
        }
        redisQueue.push(messages);
        assertEquals(count, redisQueue.size());

        List<QueueMessage> popped = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());
        for (int i = 0; i < popped.size(); i++) {
            QueueMessage msg = popped.get(i);
            // assertEquals(msg.getPriority(), i);
            System.out.println(msg.getId());
        }
    }

    @Test
    public void testDelayedPriority() {

        redisQueue.flush();
        int count = 100;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            int priority = new Random().nextInt(1000) + 1;
            priority = i + 1;
            QueueMessage msg =
                    new QueueMessage("x" + UUID.randomUUID() + "-" + priority, "Hello World-" + i);
            msg.setPriority(priority);
            msg.setTimeout(1000);
            messages.add(msg);
        }
        redisQueue.push(messages);
        assertEquals(count, redisQueue.size());

        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(1100));

        List<QueueMessage> popped = redisQueue.pop(count, 1000, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());
        int last = 0;
        for (int i = 0; i < popped.size(); i++) {
            QueueMessage msg = popped.get(i);
            System.out.println(msg.getId());
            int priority =
                    Integer.parseInt(msg.getId().substring(msg.getId().lastIndexOf('-') + 1));
            // assertTrue("Priority " + priority + " not greater than " + last, priority >= last );
            last = priority;
        }
    }

    @Test
    public void testScoreCalculation() {
        Clock clock = Clock.systemDefaultZone();
        long now = clock.millis();
        QueueMessage msg = new QueueMessage("a", null, 30, 44553333);
        double score = redisQueue.getScore(now, msg);
        System.out.println("diff: " + (score - now));
        System.out.printf("%.5f\n", score);
    }

    @Test
    public void testAck() {
        redisQueue.flush();
        redisQueue.setQueueUnackTime(1000); // 1 sec
        assertEquals(1000, redisQueue.getQueueUnackTime());

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        redisQueue.push(messages);

        assertEquals(count, redisQueue.size());
        List<QueueMessage> popped = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        // Wait for time longer than queue unack and messages should be available again!
        Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
        popped = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        // One more time, just to confirm!
        Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
        List<QueueMessage> popped2 = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped2);
        assertEquals(count, popped2.size());
        popped2.stream().forEach(msg -> redisQueue.ack(msg.getId()));

        popped2 = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped2);
        assertEquals(0, popped2.size());

        // try to ack again
        for (QueueMessage message : popped) {
            assertFalse(redisQueue.ack(message.getId()));
        }

        assertEquals(0, redisQueue.size());

        // reset it back
        redisQueue.setQueueUnackTime(30_000);
    }

    @Test
    public void testRemove() {
        redisQueue.flush();

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        redisQueue.push(messages);

        assertEquals(count, redisQueue.size());
        List<QueueMessage> popped = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(count, popped.size());

        popped.stream().forEach(msg -> redisQueue.remove(msg.getId()));
        assertEquals(0, redisQueue.size());
        popped = redisQueue.pop(count, 100, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());
    }

    @Test
    public void testAll() {

        redisQueue.flush();
        assertEquals(0, redisQueue.size());

        int count = 10;
        List<QueueMessage> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            QueueMessage msg = new QueueMessage("" + i, "Hello World-" + i);
            msg.setPriority(count - 1);
            messages.add(msg);
        }
        redisQueue.push(messages);

        long size = redisQueue.size();
        assertEquals(count, size);

        List<QueueMessage> poped = redisQueue.pop(count, 1, TimeUnit.SECONDS);
        assertNotNull(poped);
        assertEquals(count, poped.size());
        assertEquals(messages, poped);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        for (QueueMessage msg : messages) {
            QueueMessage found = redisQueue.get(msg.getId());
            assertNotNull(found);
            assertEquals(msg.getId(), found.getId());
        }
        assertNull(redisQueue.get("some fake id"));
        assertEquals(count, redisQueue.size());
        List<QueueMessage> messages3 = redisQueue.pop(count, 1, TimeUnit.SECONDS);
        if (messages3.size() < count) {
            List<QueueMessage> messages4 = redisQueue.pop(count, 1, TimeUnit.SECONDS);
            messages3.addAll(messages4);
        }
    }
}
