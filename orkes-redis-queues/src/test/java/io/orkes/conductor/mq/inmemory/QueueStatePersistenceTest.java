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

import com.google.common.util.concurrent.Uninterruptibles;

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
    public void testAsyncPersistence() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        persistence.markDirty("async-queue", () ->
                new QueueStatePersistence.QueueState("async-queue", 5000,
                        Arrays.asList(
                                new QueueStatePersistence.MessageEntry("m1", 42.0, "p1"))));

        // Wait for async write
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        QueueStatePersistence.QueueState loaded = persistence.load("async-queue");
        assertNotNull(loaded);
        assertEquals("async-queue", loaded.getQueueName());
        assertEquals(1, loaded.getMessages().size());

        persistence.shutdown();
    }

    @Test
    public void testCrashRecoverySimulation() {
        // Simulate: push messages, persist, create new queue from persisted state
        QueueStatePersistence persistence1 = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue1 = new ConductorInMemoryQueue("recovery-queue", persistence1);

        // Push messages
        List<QueueMessage> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            QueueMessage msg = new QueueMessage("msg-" + i, "payload-" + i);
            msg.setPriority(i + 1);
            messages.add(msg);
        }
        queue1.push(messages);
        assertEquals(5, queue1.size());

        // Wait for async persist
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
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
        // Wait for async delete
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        assertNull(persistence.load("delete-me"));

        persistence.shutdown();
    }

    @Test
    public void testSanitize() {
        assertEquals("abc_def_ghi", QueueStatePersistence.sanitize("abc:def/ghi"));
        assertEquals("simple", QueueStatePersistence.sanitize("simple"));
        assertEquals("a.b-c_d", QueueStatePersistence.sanitize("a.b-c_d"));
    }
}
