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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.orkes.conductor.mq.QueueMessage;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Chaos tests for the in-memory queue + disk persistence layer.
 *
 * <p>These tests inject failures (disk write failures, corrupted files, intermittent faults)
 * and verify that the queue maintains consistency and correctness under adverse conditions.
 */
public class QueueChaosTest {

    @TempDir Path tempDir;

    // -----------------------------------------------------------------------
    // 1. Disk write failure during push — in-memory stays correct
    // -----------------------------------------------------------------------

    @Test
    public void testPushSucceedsWhenDiskWriteFails() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-push", persistence);

        persistence.setFailWrites(true);

        // Push 10 messages — disk writes fail, but in-memory should be fine
        List<QueueMessage> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new QueueMessage("msg-" + i, "payload-" + i));
        }
        queue.push(messages);

        // In-memory state is correct
        assertEquals(10, queue.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(queue.exists("msg-" + i));
            QueueMessage found = queue.get("msg-" + i);
            assertNotNull(found);
            assertEquals("payload-" + i, found.getPayload());
        }

        // But disk has nothing (all writes failed)
        assertNull(persistence.load("chaos-push"));
        assertTrue(persistence.getFailedWriteCount() > 0);

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 2. Disk recovery sees last good state after write failures
    // -----------------------------------------------------------------------

    @Test
    public void testRecoveryUsesLastGoodStateAfterWriteFailures() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-recovery", persistence);

        // Phase 1: Push 5 messages successfully (disk write works)
        List<QueueMessage> batch1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch1.add(new QueueMessage("good-" + i, "payload-" + i));
        }
        queue.push(batch1);
        assertEquals(5, queue.size());

        // Verify disk has the 5 messages
        QueueStatePersistence.QueueState stateAfterBatch1 = persistence.load("chaos-recovery");
        assertNotNull(stateAfterBatch1);
        assertEquals(5, stateAfterBatch1.getMessages().size());

        // Phase 2: Disk fails — push 5 more messages
        persistence.setFailWrites(true);
        List<QueueMessage> batch2 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch2.add(new QueueMessage("bad-" + i, "payload-bad-" + i));
        }
        queue.push(batch2);

        // In-memory has all 10
        assertEquals(10, queue.size());

        // Disk still has the old 5 (batch2 write failed)
        QueueStatePersistence.QueueState stateAfterBatch2 = persistence.load("chaos-recovery");
        assertNotNull(stateAfterBatch2);
        assertEquals(5, stateAfterBatch2.getMessages().size());

        // Phase 3: Simulate crash — recover from disk
        persistence.setFailWrites(false);
        QueueStatePersistence persistence2 = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState diskState = persistence2.load("chaos-recovery");
        ConductorInMemoryQueue recovered =
                new ConductorInMemoryQueue("chaos-recovery", persistence2, diskState);

        // Recovered queue has only the 5 from batch1 (batch2 was lost)
        assertEquals(5, recovered.size());
        for (int i = 0; i < 5; i++) {
            assertTrue(recovered.exists("good-" + i));
        }
        for (int i = 0; i < 5; i++) {
            assertFalse(recovered.exists("bad-" + i));
        }

        persistence.shutdown();
        persistence2.shutdown();
    }

    // -----------------------------------------------------------------------
    // 3. Pop during disk failure — message re-score lost on recovery
    // -----------------------------------------------------------------------

    @Test
    public void testPopDuringDiskFailureCausesRedeliveryOnRecovery() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-pop", persistence);

        // Push a message and let it persist
        queue.push(Arrays.asList(new QueueMessage("redelivery-1", "data")));

        QueueStatePersistence.QueueState prePopState = persistence.load("chaos-pop");
        assertNotNull(prePopState);
        double prePopscore = prePopState.getMessages().get(0).getScore();

        // Fail disk, then pop (re-scores the message to future)
        persistence.setFailWrites(true);
        List<QueueMessage> popped = queue.pop(1, 0, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size());

        // In-memory: message is re-scored to the future (invisible)
        // Disk: still has old score (pop's write failed)
        QueueStatePersistence.QueueState postPopState = persistence.load("chaos-pop");
        assertEquals(prePopscore, postPopState.getMessages().get(0).getScore(),
                "Disk should still have pre-pop score since write failed");

        // Simulate crash — recover from disk
        QueueStatePersistence persistence2 = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState diskState = persistence2.load("chaos-pop");
        ConductorInMemoryQueue recovered =
                new ConductorInMemoryQueue("chaos-pop", persistence2, diskState);

        // The message should be re-deliverable (old score, which is in the past)
        List<QueueMessage> redelivered = recovered.pop(1, 0, TimeUnit.MILLISECONDS);
        assertEquals(1, redelivered.size(), "Message should be redelivered after crash recovery");
        assertEquals("redelivery-1", redelivered.get(0).getId());

        persistence.shutdown();
        persistence2.shutdown();
    }

    // -----------------------------------------------------------------------
    // 4. Ack during disk failure — message reappears on recovery
    // -----------------------------------------------------------------------

    @Test
    public void testAckDuringDiskFailureCausesMessageReappearOnRecovery() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-ack", persistence);

        // Push and pop a message (both persisted)
        queue.push(Arrays.asList(new QueueMessage("ack-me", "data")));
        List<QueueMessage> popped = queue.pop(1, 0, TimeUnit.MILLISECONDS);
        assertEquals(1, popped.size());

        // Fail disk, then ack
        persistence.setFailWrites(true);
        assertTrue(queue.ack("ack-me"));

        // In-memory: message is gone
        assertEquals(0, queue.size());
        assertFalse(queue.exists("ack-me"));

        // Disk: message still present (ack write failed)
        QueueStatePersistence.QueueState diskState = persistence.load("chaos-ack");
        assertNotNull(diskState);
        assertEquals(1, diskState.getMessages().size(),
                "Disk should still have the message since ack write failed");

        // Simulate crash — message reappears
        QueueStatePersistence persistence2 = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState loadedState = persistence2.load("chaos-ack");
        ConductorInMemoryQueue recovered =
                new ConductorInMemoryQueue("chaos-ack", persistence2, loadedState);
        assertEquals(1, recovered.size(), "Message should reappear after crash since ack wasn't persisted");

        persistence.shutdown();
        persistence2.shutdown();
    }

    // -----------------------------------------------------------------------
    // 5. Writes resume after failure — full state is captured
    // -----------------------------------------------------------------------

    @Test
    public void testWriteResumptionCapturesFullState() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-resume", persistence);

        // Push 3 messages with disk working
        for (int i = 0; i < 3; i++) {
            queue.push(Arrays.asList(new QueueMessage("initial-" + i, "data-" + i)));
        }

        // Fail disk, push 3 more
        persistence.setFailWrites(true);
        for (int i = 0; i < 3; i++) {
            queue.push(Arrays.asList(new QueueMessage("failed-" + i, "data-" + i)));
        }

        // Disk still has only 3
        QueueStatePersistence.QueueState staleState = persistence.load("chaos-resume");
        assertEquals(3, staleState.getMessages().size());

        // Resume disk, push 1 more — this write captures ALL 7 messages
        persistence.setFailWrites(false);
        queue.push(Arrays.asList(new QueueMessage("trigger-write", "data")));

        QueueStatePersistence.QueueState freshState = persistence.load("chaos-resume");
        assertNotNull(freshState);
        assertEquals(7, freshState.getMessages().size(),
                "After writes resume, the next successful write should capture full state");

        // Verify all messages are present
        Set<String> ids = freshState.getMessages().stream()
                .map(QueueStatePersistence.MessageEntry::getId)
                .collect(Collectors.toSet());
        for (int i = 0; i < 3; i++) {
            assertTrue(ids.contains("initial-" + i));
            assertTrue(ids.contains("failed-" + i));
        }
        assertTrue(ids.contains("trigger-write"));

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 6. Intermittent failures — every Nth write fails
    // -----------------------------------------------------------------------

    @Test
    public void testIntermittentWriteFailures() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-intermittent", persistence);

        // Fail every 3rd write
        persistence.setFailEveryNthWrite(3);

        // Push 10 messages one at a time (10 writes, ~3 will fail)
        for (int i = 0; i < 10; i++) {
            queue.push(Arrays.asList(new QueueMessage("msg-" + i, "data-" + i)));
        }

        // In-memory always has all 10
        assertEquals(10, queue.size());

        // Some writes failed
        assertTrue(persistence.getFailedWriteCount() >= 3);

        // Disk state exists (at least some writes succeeded)
        QueueStatePersistence.QueueState state = persistence.load("chaos-intermittent");
        assertNotNull(state);
        // The last successful write should have captured the state at that point
        assertTrue(state.getMessages().size() > 0);

        // The last write (msg-9) was write #10, which is NOT a multiple of 3, so it succeeded.
        // That write captures all 10 messages.
        assertEquals(10, state.getMessages().size(),
                "Last write succeeded and should capture full state");

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 7. Corrupted JSON on disk — load returns null gracefully
    // -----------------------------------------------------------------------

    @Test
    public void testCorruptedJsonFileOnDisk() throws IOException {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        // Write garbage to the state file
        Path corruptedFile = tempDir.resolve("corrupted-queue.json");
        Files.write(corruptedFile, "{{{{not valid json!!!!".getBytes(StandardCharsets.UTF_8));

        // Load should return null, not throw
        QueueStatePersistence.QueueState state = persistence.load("corrupted-queue");
        assertNull(state, "Corrupted JSON should return null, not crash");

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 8. Truncated JSON on disk — load returns null gracefully
    // -----------------------------------------------------------------------

    @Test
    public void testTruncatedJsonFileOnDisk() throws IOException {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        // Write a valid start but truncated JSON
        String truncated = "{\"queueName\":\"truncated-q\",\"queueUnackTime\":30000,\"messages\":[{\"id\":\"m1\"";
        Path truncatedFile = tempDir.resolve("truncated-q.json");
        Files.write(truncatedFile, truncated.getBytes(StandardCharsets.UTF_8));

        QueueStatePersistence.QueueState state = persistence.load("truncated-q");
        assertNull(state, "Truncated JSON should return null, not crash");

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 9. Empty file on disk — load returns null gracefully
    // -----------------------------------------------------------------------

    @Test
    public void testEmptyFileOnDisk() throws IOException {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        Path emptyFile = tempDir.resolve("empty-queue.json");
        Files.write(emptyFile, new byte[0]);

        QueueStatePersistence.QueueState state = persistence.load("empty-queue");
        assertNull(state, "Empty file should return null, not crash");

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 10. Stale .tmp file left behind — doesn't interfere with operations
    // -----------------------------------------------------------------------

    @Test
    public void testStaleTmpFileDoesNotInterfere() throws IOException {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        // Simulate a crash that left a .tmp file behind
        Path tmpFile = tempDir.resolve("stale-queue.json.tmp");
        Files.write(tmpFile, "stale garbage data".getBytes(StandardCharsets.UTF_8));

        // Normal operations should work fine
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("stale-queue", persistence);
        queue.push(Arrays.asList(new QueueMessage("fresh-1", "data")));

        QueueStatePersistence.QueueState state = persistence.load("stale-queue");
        assertNotNull(state);
        assertEquals(1, state.getMessages().size());
        assertEquals("fresh-1", state.getMessages().get(0).getId());

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 11. Flush during disk failure — delete fails, file persists
    // -----------------------------------------------------------------------

    @Test
    public void testFlushDuringDeleteFailureLeavesStaleFile() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-flush", persistence);

        // Push data and persist
        queue.push(Arrays.asList(new QueueMessage("flush-me", "data")));
        assertNotNull(persistence.load("chaos-flush"));

        // Fail deletes, then flush
        persistence.setFailDeletes(true);
        queue.flush();

        // In-memory is empty
        assertEquals(0, queue.size());

        // But disk file is still there (delete failed)
        QueueStatePersistence.QueueState state = persistence.load("chaos-flush");
        assertNotNull(state, "Disk file should survive failed delete");
        assertEquals(1, state.getMessages().size());

        // On recovery, the stale messages come back
        persistence.setFailDeletes(false);
        QueueStatePersistence persistence2 = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState diskState = persistence2.load("chaos-flush");
        ConductorInMemoryQueue recovered =
                new ConductorInMemoryQueue("chaos-flush", persistence2, diskState);
        assertEquals(1, recovered.size(), "Flushed data reappears if delete failed");

        persistence.shutdown();
        persistence2.shutdown();
    }

    // -----------------------------------------------------------------------
    // 12. Concurrent push/pop with intermittent disk failures
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentOperationsWithIntermittentFailures()
            throws InterruptedException {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-concurrent", persistence);

        // Fail every 5th write
        persistence.setFailEveryNthWrite(5);

        int messageCount = 100;
        CountDownLatch pushLatch = new CountDownLatch(messageCount);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Push 100 messages concurrently
        for (int i = 0; i < messageCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    queue.push(Arrays.asList(
                            new QueueMessage("concurrent-" + idx, "data-" + idx)));
                } finally {
                    pushLatch.countDown();
                }
            });
        }
        assertTrue(pushLatch.await(10, TimeUnit.SECONDS), "Pushes should complete in time");

        // All 100 messages should be in-memory regardless of disk failures
        assertEquals(messageCount, queue.size());

        // Pop all and verify no duplicates
        List<QueueMessage> allPopped = new ArrayList<>();
        while (allPopped.size() < messageCount) {
            List<QueueMessage> popped = queue.pop(messageCount, 0, TimeUnit.MILLISECONDS);
            if (popped.isEmpty()) break;
            allPopped.addAll(popped);
        }
        assertEquals(messageCount, allPopped.size());

        Set<String> uniqueIds = allPopped.stream()
                .map(QueueMessage::getId)
                .collect(Collectors.toSet());
        assertEquals(messageCount, uniqueIds.size(), "No duplicates in popped messages");

        // Some writes failed
        assertTrue(persistence.getFailedWriteCount() > 0);

        executor.shutdown();
        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 13. Recovery from corrupted file, then normal operation resumes
    // -----------------------------------------------------------------------

    @Test
    public void testRecoveryFromCorruptedFileThenNormalOperation() throws IOException {
        // First: write valid state
        QueueStatePersistence setup = new QueueStatePersistence(tempDir);
        ConductorInMemoryQueue setupQueue = new ConductorInMemoryQueue("corrupt-recover", setup);
        setupQueue.push(Arrays.asList(new QueueMessage("before-corrupt", "data")));
        setup.shutdown();

        // Corrupt the file
        Path stateFile = tempDir.resolve("corrupt-recover.json");
        Files.write(stateFile, "CORRUPTED!!!".getBytes(StandardCharsets.UTF_8));

        // Load returns null for corrupted file
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);
        QueueStatePersistence.QueueState state = persistence.load("corrupt-recover");
        assertNull(state, "Corrupted file should not load");

        // Start fresh queue (no initial state since load returned null)
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("corrupt-recover", persistence);
        assertEquals(0, queue.size());

        // Push new messages — this overwrites the corrupted file with valid state
        queue.push(Arrays.asList(new QueueMessage("after-corrupt", "data")));

        QueueStatePersistence.QueueState newState = persistence.load("corrupt-recover");
        assertNotNull(newState);
        assertEquals(1, newState.getMessages().size());
        assertEquals("after-corrupt", newState.getMessages().get(0).getId());

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 14. Mixed operation sequence with failures — consistency check
    // -----------------------------------------------------------------------

    @Test
    public void testMixedOperationSequenceWithFailures() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-mixed", persistence);

        // Step 1: Push 5 messages (disk works)
        for (int i = 0; i < 5; i++) {
            queue.push(Arrays.asList(new QueueMessage("m-" + i, "data-" + i)));
        }
        assertEquals(5, queue.size());

        // Step 2: Pop 2 messages (disk works — re-scored to future)
        List<QueueMessage> popped = queue.pop(2, 0, TimeUnit.MILLISECONDS);
        assertEquals(2, popped.size());
        Set<String> poppedIds = popped.stream().map(QueueMessage::getId).collect(Collectors.toSet());

        // Step 3: Disk fails — ack the 2 popped messages
        persistence.setFailWrites(true);
        for (QueueMessage msg : popped) {
            queue.ack(msg.getId());
        }
        assertEquals(3, queue.size()); // in-memory: 5 - 2 acked = 3

        // Step 4: Disk fails — push 2 new messages
        queue.push(Arrays.asList(
                new QueueMessage("new-0", "newdata-0"),
                new QueueMessage("new-1", "newdata-1")));
        assertEquals(5, queue.size()); // in-memory: 3 + 2 = 5

        // Disk state still has the state from step 2 (5 messages, 2 re-scored)
        QueueStatePersistence.QueueState diskState = persistence.load("chaos-mixed");
        assertNotNull(diskState);
        assertEquals(5, diskState.getMessages().size()); // all 5 from step 2

        // Step 5: Resume disk, do one more operation to force a write
        persistence.setFailWrites(false);
        queue.push(Arrays.asList(new QueueMessage("trigger", "data")));

        // Now disk should have the full correct state: 3 original + 2 new + 1 trigger = 6
        QueueStatePersistence.QueueState finalState = persistence.load("chaos-mixed");
        assertNotNull(finalState);
        assertEquals(6, finalState.getMessages().size());

        // Verify the acked messages are NOT on disk
        Set<String> diskIds = finalState.getMessages().stream()
                .map(QueueStatePersistence.MessageEntry::getId)
                .collect(Collectors.toSet());
        for (String ackedId : poppedIds) {
            assertFalse(diskIds.contains(ackedId),
                    "Acked message " + ackedId + " should not be on disk after successful write");
        }
        assertTrue(diskIds.contains("new-0"));
        assertTrue(diskIds.contains("new-1"));
        assertTrue(diskIds.contains("trigger"));

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 15. Rapid push/ack cycle with failures — no state leaks
    // -----------------------------------------------------------------------

    @Test
    public void testRapidPushAckCycleWithFailures() {
        ChaosQueueStatePersistence persistence = new ChaosQueueStatePersistence(tempDir);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("chaos-rapid", persistence);

        // Fail every 2nd write
        persistence.setFailEveryNthWrite(2);

        // Rapid push-pop-ack cycle
        for (int i = 0; i < 50; i++) {
            String id = "rapid-" + i;
            queue.push(Arrays.asList(new QueueMessage(id, "data")));
            List<QueueMessage> popped = queue.pop(1, 0, TimeUnit.MILLISECONDS);
            assertEquals(1, popped.size());
            assertTrue(queue.ack(id));
        }

        // In-memory should be empty (all messages were acked)
        assertEquals(0, queue.size());

        // Some writes failed
        assertTrue(persistence.getFailedWriteCount() > 0);

        // Do one more push to trigger a clean write
        persistence.setFailEveryNthWrite(0); // disable failures
        queue.push(Arrays.asList(new QueueMessage("final", "data")));

        // Disk should show exactly 1 message
        QueueStatePersistence.QueueState state = persistence.load("chaos-rapid");
        assertNotNull(state);
        assertEquals(1, state.getMessages().size());
        assertEquals("final", state.getMessages().get(0).getId());

        persistence.shutdown();
    }

    // -----------------------------------------------------------------------
    // 16. loadAll with mix of valid and corrupted files
    // -----------------------------------------------------------------------

    @Test
    public void testLoadAllWithMixedValidAndCorruptedFiles() throws IOException {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir);

        // Write valid state for two queues
        persistence.persistNow("good-queue-1",
                new QueueStatePersistence.QueueState("good-queue-1", 1000,
                        Arrays.asList(new QueueStatePersistence.MessageEntry("g1", 1.0, "p1"))));
        persistence.persistNow("good-queue-2",
                new QueueStatePersistence.QueueState("good-queue-2", 2000,
                        Arrays.asList(new QueueStatePersistence.MessageEntry("g2", 2.0, "p2"))));

        // Write corrupted file
        Files.write(tempDir.resolve("bad-queue.json"),
                "NOT JSON AT ALL".getBytes(StandardCharsets.UTF_8));

        // Write empty file
        Files.write(tempDir.resolve("empty-queue.json"), new byte[0]);

        // loadAll should return the 2 good queues and skip the bad ones
        Map<String, QueueStatePersistence.QueueState> all = persistence.loadAll();
        assertEquals(2, all.size(), "Should load only the 2 valid queue states");
        assertNotNull(all.get("good-queue-1"));
        assertNotNull(all.get("good-queue-2"));

        persistence.shutdown();
    }
}
