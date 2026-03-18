/*
 * Copyright 2024 Orkes, Inc.
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
package io.orkes.conductor.mq.inmemory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.orkes.conductor.mq.QueueMessage;

import static org.junit.jupiter.api.Assertions.*;

public class QueueStatePersistenceTest {

    @TempDir Path tempDir;

    @Test
    public void testWriteAndLoadRoundTrip() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        List<QueueStatePersistence.MessageEntry> entries = new ArrayList<>();
        entries.add(new QueueStatePersistence.MessageEntry("msg1", 100.5, "payload1"));
        entries.add(new QueueStatePersistence.MessageEntry("msg2", 200.3, "payload2"));
        entries.add(new QueueStatePersistence.MessageEntry("msg3", 300.0, null));

        QueueStatePersistence.QueueState state =
                new QueueStatePersistence.QueueState("test-queue", 30000, entries);

        persistence.writeSync("test-queue", state);

        QueueStatePersistence.QueueState loaded = persistence.load("test-queue");
        assertNotNull(loaded);
        assertEquals("test-queue", loaded.getQueueName());
        assertEquals(30000, loaded.getQueueUnackTime());
        assertEquals(3, loaded.getMessages().size());
        assertEquals("msg1", loaded.getMessages().get(0).getId());
        assertEquals(100.5, loaded.getMessages().get(0).getScore());
        assertEquals("payload1", loaded.getMessages().get(0).getPayload());
        assertNull(loaded.getMessages().get(2).getPayload());

        persistence.shutdown();
    }

    @Test
    public void testLoadNonExistent() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        assertNull(persistence.load("nonexistent-queue"));
        persistence.shutdown();
    }

    @Test
    public void testLoadAll() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        persistence.writeSync("queue-a",
                new QueueStatePersistence.QueueState("queue-a", 1000,
                        Arrays.asList(
                                new QueueStatePersistence.MessageEntry("a1", 1.0, "pa"))));
        persistence.writeSync("queue-b",
                new QueueStatePersistence.QueueState("queue-b", 2000,
                        Arrays.asList(
                                new QueueStatePersistence.MessageEntry("b1", 2.0, "pb"))));

        Map<String, QueueStatePersistence.QueueState> all = persistence.loadAll();
        assertEquals(2, all.size());
        assertNotNull(all.get("queue-a"));
        assertNotNull(all.get("queue-b"));
        assertEquals("a1", all.get("queue-a").getMessages().get(0).getId());

        persistence.shutdown();
    }

    @Test
    public void testSyncPersistence() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        QueueStatePersistence.QueueState state =
                new QueueStatePersistence.QueueState("sync-queue", 5000,
                        Arrays.asList(
                                new QueueStatePersistence.MessageEntry("m1", 42.0, "p1")));
        persistence.persistNow("sync-queue", state);

        // No sleep needed — persistNow is synchronous
        QueueStatePersistence.QueueState loaded = persistence.load("sync-queue");
        assertNotNull(loaded);
        assertEquals("sync-queue", loaded.getQueueName());
        assertEquals(1, loaded.getMessages().size());

        persistence.shutdown();
    }

    @Test
    public void testCrashRecoverySimulation() {
        // Simulate: push messages, persist, create new queue from persisted state
        QueueStatePersistence persistence1 = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue1 = new ConductorInMemoryQueue("recovery-queue", persistence1);

        // Push messages — persistence is synchronous, so state is durable immediately
        List<QueueMessage> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            QueueMessage msg = new QueueMessage("msg-" + i, "payload-" + i);
            msg.setPriority(i + 1);
            messages.add(msg);
        }
        queue1.push(messages);
        assertEquals(5, queue1.size());

        // No sleep needed — push persists synchronously
        persistence1.shutdown();

        // "Restart": create new persistence and queue, hydrating from disk
        QueueStatePersistence persistence2 = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState state = persistence2.load("recovery-queue");
        assertNotNull(state);

        ConductorInMemoryQueue queue2 =
                new ConductorInMemoryQueue("recovery-queue", persistence2, state);
        assertEquals(5, queue2.size());

        // Pop all messages and verify
        List<QueueMessage> popped = queue2.pop(5, 100, TimeUnit.MILLISECONDS);
        assertEquals(5, popped.size());

        persistence2.shutdown();
    }

    @Test
    public void testDeleteQueueState() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        persistence.writeSync("delete-me",
                new QueueStatePersistence.QueueState("delete-me", 1000, Arrays.asList()));

        assertNotNull(persistence.load("delete-me"));

        persistence.delete("delete-me");
        // No sleep needed — delete is synchronous
        assertNull(persistence.load("delete-me"));

        persistence.shutdown();
    }

    @Test
    public void testPushIsDurableImmediately() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("durable-push", persistence);

        // Push a message — should be on disk immediately with no sleep
        QueueMessage msg = new QueueMessage("d1", "payload-d1");
        queue.push(Arrays.asList(msg));

        QueueStatePersistence.QueueState loaded = persistence.load("durable-push");
        assertNotNull(loaded, "State must be on disk immediately after push");
        assertEquals(1, loaded.getMessages().size());
        assertEquals("d1", loaded.getMessages().get(0).getId());
        assertEquals("payload-d1", loaded.getMessages().get(0).getPayload());

        persistence.shutdown();
    }

    @Test
    public void testPopUpdateIsDurableImmediately() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("durable-pop", persistence);

        queue.push(Arrays.asList(new QueueMessage("p1", "data")));

        // Pop re-scores the message (makes it invisible) — verify disk state reflects this
        List<QueueMessage> popped = queue.pop(1, 0, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size());

        QueueStatePersistence.QueueState loaded = persistence.load("durable-pop");
        assertNotNull(loaded, "State must be on disk immediately after pop");
        assertEquals(1, loaded.getMessages().size());
        // The score should now be in the future (now + queueUnackTime)
        assertTrue(loaded.getMessages().get(0).getScore() > System.currentTimeMillis() - 1000,
                "Score should have been updated by pop");

        persistence.shutdown();
    }

    @Test
    public void testAckIsDurableImmediately() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("durable-ack", persistence);

        queue.push(Arrays.asList(new QueueMessage("a1", "data")));
        queue.pop(1, 0, TimeUnit.MILLISECONDS);

        // Ack removes message — verify disk state is empty
        queue.ack("a1");

        QueueStatePersistence.QueueState loaded = persistence.load("durable-ack");
        assertNotNull(loaded);
        assertEquals(0, loaded.getMessages().size(),
                "Acked message must be removed from disk immediately");

        persistence.shutdown();
    }

    @Test
    public void testSetUnacktimeoutIsDurableImmediately() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("durable-timeout", persistence);

        queue.push(Arrays.asList(new QueueMessage("t1", "data")));

        // Update the unack timeout — verify disk state reflects the new score
        queue.setUnacktimeout("t1", 60_000);

        QueueStatePersistence.QueueState loaded = persistence.load("durable-timeout");
        assertNotNull(loaded);
        assertEquals(1, loaded.getMessages().size());
        assertTrue(loaded.getMessages().get(0).getScore() > System.currentTimeMillis() + 50_000,
                "Score should reflect updated unack timeout on disk");

        persistence.shutdown();
    }

    @Test
    public void testSanitize() {
        assertEquals("abc_def_ghi", QueueStatePersistence.sanitize("abc:def/ghi"));
        assertEquals("simple", QueueStatePersistence.sanitize("simple"));
        assertEquals("a.b-c_d", QueueStatePersistence.sanitize("a.b-c_d"));
    }
}
