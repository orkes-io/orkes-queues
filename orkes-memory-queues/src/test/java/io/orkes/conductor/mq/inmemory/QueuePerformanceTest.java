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
import java.util.concurrent.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import io.orkes.conductor.mq.QueueMessage;

/** Performance benchmarks comparing snapshot-based persistence vs WAL-based persistence. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QueuePerformanceTest {

    @TempDir Path tempDir;

    // ── Snapshot-based benchmarks (baseline) ────────────────────────────

    @Test
    @Order(1)
    void benchSnapshotPush() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir.resolve("snap-push"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        int count = 5_000;
        warmUp(queue);

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
        }
        long elapsed = System.nanoTime() - start;
        persistence.shutdown();

        printResult("Snapshot Push", count, elapsed);
    }

    @Test
    @Order(2)
    void benchWalPush() {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-push"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        int count = 5_000;
        warmUp(queue);

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
        }
        long elapsed = System.nanoTime() - start;

        printResult("WAL Push", count, elapsed);
    }

    @Test
    @Order(3)
    void benchSnapshotAck() {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir.resolve("snap-ack"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        int count = 5_000;
        prepopAndPop(queue, count);

        List<String> ids = collectIds(queue, count);

        long start = System.nanoTime();
        for (String id : ids) {
            queue.ack(id);
        }
        long elapsed = System.nanoTime() - start;
        persistence.shutdown();

        printResult("Snapshot Ack", count, elapsed);
    }

    @Test
    @Order(4)
    void benchWalAck() {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-ack"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        int count = 5_000;
        prepopAndPop(queue, count);

        List<String> ids = collectIds(queue, count);

        long start = System.nanoTime();
        for (String id : ids) {
            queue.ack(id);
        }
        long elapsed = System.nanoTime() - start;

        printResult("WAL Ack", count, elapsed);
    }

    @Test
    @Order(5)
    void benchSnapshotPushPopAckCycle() {
        QueueStatePersistence persistence =
                new QueueStatePersistence(tempDir.resolve("snap-cycle"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        int count = 3_000;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
            List<QueueMessage> msgs = queue.pop(1, 0, TimeUnit.MILLISECONDS);
            if (!msgs.isEmpty()) queue.ack(msgs.get(0).getId());
        }
        long elapsed = System.nanoTime() - start;
        persistence.shutdown();

        printResult("Snapshot Push+Pop+Ack", count, elapsed);
    }

    @Test
    @Order(6)
    void benchWalPushPopAckCycle() {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-cycle"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        int count = 3_000;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
            List<QueueMessage> msgs = queue.pop(1, 0, TimeUnit.MILLISECONDS);
            if (!msgs.isEmpty()) queue.ack(msgs.get(0).getId());
        }
        long elapsed = System.nanoTime() - start;

        printResult("WAL Push+Pop+Ack", count, elapsed);
    }

    // ── Large queue tests ───────────────────────────────────────────────

    @Test
    @Order(7)
    void benchSnapshotLargeQueuePush() {
        QueueStatePersistence persistence =
                new QueueStatePersistence(tempDir.resolve("snap-large"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        // Preload 10K messages
        for (int i = 0; i < 10_000; i++) {
            queue.push(List.of(new QueueMessage("preload-" + i, "payload-" + i, 0)));
        }

        int count = 1_000;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("new-" + i, "payload-" + i, 0)));
        }
        long elapsed = System.nanoTime() - start;
        persistence.shutdown();

        printResult("Snapshot Push (10K queue)", count, elapsed);
    }

    @Test
    @Order(8)
    void benchWalLargeQueuePush() {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-large"), 50_000);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        // Preload 10K messages
        for (int i = 0; i < 10_000; i++) {
            queue.push(List.of(new QueueMessage("preload-" + i, "payload-" + i, 0)));
        }

        int count = 1_000;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("new-" + i, "payload-" + i, 0)));
        }
        long elapsed = System.nanoTime() - start;

        printResult("WAL Push (10K queue)", count, elapsed);
    }

    // ── Batch push ──────────────────────────────────────────────────────

    @Test
    @Order(9)
    void benchSnapshotBatchPush() {
        QueueStatePersistence persistence =
                new QueueStatePersistence(tempDir.resolve("snap-batch"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        int totalMessages = 10_000;
        int batchSize = 50;
        int batches = totalMessages / batchSize;

        long start = System.nanoTime();
        for (int b = 0; b < batches; b++) {
            List<QueueMessage> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(new QueueMessage("msg-" + b + "-" + i, "payload", 0));
            }
            queue.push(batch);
        }
        long elapsed = System.nanoTime() - start;
        persistence.shutdown();

        double opsPerSec = totalMessages / (elapsed / 1_000_000_000.0);
        System.out.printf(
                "[PERF] Snapshot Batch Push (size=%d): %.0f msgs/sec%n", batchSize, opsPerSec);
    }

    @Test
    @Order(10)
    void benchWalBatchPush() {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-batch"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        int totalMessages = 10_000;
        int batchSize = 50;
        int batches = totalMessages / batchSize;

        long start = System.nanoTime();
        for (int b = 0; b < batches; b++) {
            List<QueueMessage> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(new QueueMessage("msg-" + b + "-" + i, "payload", 0));
            }
            queue.push(batch);
        }
        long elapsed = System.nanoTime() - start;

        double opsPerSec = totalMessages / (elapsed / 1_000_000_000.0);
        System.out.printf("[PERF] WAL Batch Push (size=%d): %.0f msgs/sec%n", batchSize, opsPerSec);
    }

    // ── Concurrent benchmarks ───────────────────────────────────────────

    @Test
    @Order(11)
    void benchSnapshotConcurrentPush() throws Exception {
        QueueStatePersistence persistence = new QueueStatePersistence(tempDir.resolve("snap-conc"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", persistence);
        queue.setQueueUnackTime(60_000);

        long elapsed = runConcurrentPush(queue, 4, 2_000);
        persistence.shutdown();

        double opsPerSec = 8_000 / (elapsed / 1_000_000_000.0);
        System.out.printf("[PERF] Snapshot Concurrent Push (4 threads): %.0f ops/sec%n", opsPerSec);
    }

    @Test
    @Order(12)
    void benchWalConcurrentPush() throws Exception {
        WriteAheadLog wal = new WriteAheadLog(tempDir.resolve("wal-conc"));
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        long elapsed = runConcurrentPush(queue, 4, 2_000);

        double opsPerSec = 8_000 / (elapsed / 1_000_000_000.0);
        System.out.printf("[PERF] WAL Concurrent Push (4 threads): %.0f ops/sec%n", opsPerSec);
    }

    // ── No-persistence baseline ─────────────────────────────────────────

    @Test
    @Order(13)
    void benchNoPersistencePush() {
        ConductorInMemoryQueue queue =
                new ConductorInMemoryQueue("bench", (QueueStatePersistence) null);
        queue.setQueueUnackTime(60_000);

        int count = 5_000;
        warmUp(queue);

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
        }
        long elapsed = System.nanoTime() - start;

        printResult("No-Persist Push", count, elapsed);
    }

    // ── WAL recovery benchmark ──────────────────────────────────────────

    @Test
    @Order(14)
    void benchWalRecovery() {
        Path walDir = tempDir.resolve("wal-recovery");
        WriteAheadLog wal = new WriteAheadLog(walDir, 50_000);
        ConductorInMemoryQueue queue = new ConductorInMemoryQueue("bench", wal);
        queue.setQueueUnackTime(60_000);

        // Write 5000 messages
        for (int i = 0; i < 5_000; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
        }

        // Measure recovery time
        WriteAheadLog wal2 = new WriteAheadLog(walDir, 50_000);
        long start = System.nanoTime();
        QueueStatePersistence.QueueState recovered = wal2.load("bench");
        long elapsed = System.nanoTime() - start;

        double avgUs = (elapsed / 1_000.0);
        System.out.printf(
                "[PERF] WAL Recovery (5000 entries): %.1f µs, %d messages recovered%n",
                avgUs, recovered != null ? recovered.getMessages().size() : 0);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private void warmUp(ConductorInMemoryQueue queue) {
        for (int i = 0; i < 100; i++) {
            queue.push(List.of(new QueueMessage("warmup-" + i, "payload", 0)));
        }
        queue.flush();
    }

    private void prepopAndPop(ConductorInMemoryQueue queue, int count) {
        for (int i = 0; i < count; i++) {
            queue.push(List.of(new QueueMessage("msg-" + i, "payload-" + i, 0)));
        }
        // Pop all so they can be acked
        int popped = 0;
        while (popped < count) {
            List<QueueMessage> msgs = queue.pop(100, 0, TimeUnit.MILLISECONDS);
            popped += msgs.size();
        }
    }

    private List<String> collectIds(ConductorInMemoryQueue queue, int count) {
        // Messages are already popped (rescored with high timeout), need to get their IDs
        // Since they were pushed as msg-0..msg-(count-1), we know the IDs
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add("msg-" + i);
        }
        return ids;
    }

    private long runConcurrentPush(ConductorInMemoryQueue queue, int threads, int perThread)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(
                    pool.submit(
                            () -> {
                                try {
                                    latch.await();
                                } catch (InterruptedException e) {
                                    return;
                                }
                                for (int i = 0; i < perThread; i++) {
                                    queue.push(
                                            List.of(
                                                    new QueueMessage(
                                                            "t" + threadId + "-" + i,
                                                            "payload",
                                                            0)));
                                }
                            }));
        }

        long start = System.nanoTime();
        latch.countDown();
        for (Future<?> f : futures) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();
        return elapsed;
    }

    private void printResult(String label, int count, long elapsedNs) {
        double opsPerSec = count / (elapsedNs / 1_000_000_000.0);
        double avgLatencyUs = (elapsedNs / 1_000.0) / count;
        System.out.printf(
                "[PERF] %s: %.0f ops/sec, avg %.1f µs (%d ops)%n",
                label, opsPerSec, avgLatencyUs, count);
    }
}
