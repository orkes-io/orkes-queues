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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test helper that extends {@link QueueStatePersistence} with injectable failure modes. Use this to
 * simulate disk I/O failures, intermittent faults, and write latency in tests.
 */
public class ChaosQueueStatePersistence extends QueueStatePersistence {

    private final AtomicBoolean failWrites = new AtomicBoolean(false);
    private final AtomicBoolean failDeletes = new AtomicBoolean(false);
    private final AtomicInteger writeCount = new AtomicInteger(0);
    private final AtomicInteger failedWriteCount = new AtomicInteger(0);
    private final AtomicInteger deleteCount = new AtomicInteger(0);
    private volatile int failEveryNthWrite = 0; // 0 = disabled

    public ChaosQueueStatePersistence(Path dataDir) {
        super(dataDir);
    }

    @Override
    public void persistNow(String queueName, QueueState state) {
        int count = writeCount.incrementAndGet();
        if (failWrites.get()) {
            failedWriteCount.incrementAndGet();
            // Simulate I/O failure: silently drop the write (same as IOException in writeToFile)
            return;
        }
        if (failEveryNthWrite > 0 && count % failEveryNthWrite == 0) {
            failedWriteCount.incrementAndGet();
            return;
        }
        super.persistNow(queueName, state);
    }

    @Override
    public void delete(String queueName) {
        deleteCount.incrementAndGet();
        if (failDeletes.get()) {
            return;
        }
        super.delete(queueName);
    }

    /**
     * Enable or disable write failures. When enabled, all persistNow calls are silently dropped.
     */
    public void setFailWrites(boolean fail) {
        failWrites.set(fail);
    }

    /** Enable or disable delete failures. When enabled, all delete calls are silently dropped. */
    public void setFailDeletes(boolean fail) {
        failDeletes.set(fail);
    }

    /**
     * Fail every Nth write. Set to 0 to disable. For example, failEveryNthWrite=3 means writes 3,
     * 6, 9, ... will fail while others succeed.
     */
    public void setFailEveryNthWrite(int n) {
        this.failEveryNthWrite = n;
    }

    /** Total number of persistNow calls (both successful and failed). */
    public int getWriteCount() {
        return writeCount.get();
    }

    /** Number of writes that were intentionally dropped by chaos injection. */
    public int getFailedWriteCount() {
        return failedWriteCount.get();
    }

    /** Total number of delete calls. */
    public int getDeleteCount() {
        return deleteCount.get();
    }

    /** Reset all counters. */
    public void resetCounters() {
        writeCount.set(0);
        failedWriteCount.set(0);
        deleteCount.set(0);
    }
}
