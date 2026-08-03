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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.orkes.conductor.mq.QueueMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the per-queue poller slot ({@code activePollers}) and the backoff hand-off that used to
 * corrupt it: {@code wakePoller()} gated on {@code cancel(false)}, which succeeds on an
 * already-running task, so a wake and the firing backoff body could both submit — two poller chains
 * for one slot, both released when demand hit zero, count negative, queue dead for good.
 *
 * <p>No Redis needed: {@link QueueMonitor} is driven through a fake fetch over an in-memory
 * backlog. Reflection only observes or perturbs internal state; nothing in production exists for
 * these tests.
 */
public class QueueMonitorPollerSlotTest {

    private final List<CountingExecutor> executors =
            Collections.synchronizedList(new ArrayList<>());

    @AfterEach
    public void shutdownExecutors() {
        for (CountingExecutor executor : executors) {
            executor.shutdownNow();
        }
        executors.clear();
    }

    // ------------------------------------------------------------------------------------------
    // 1. Reproduction of the stall.
    // ------------------------------------------------------------------------------------------

    /**
     * Reproduces the stall at the mechanism rather than the timing: inject the two-chains-one-slot
     * state the race leaves behind, then drop demand. Pre-fix this lands on {@code -1} and the
     * queue delivers nothing thereafter.
     *
     * <p>The race that creates the duplicate chain is nanoseconds wide (hence the hours to hit it
     * in production) and is not reproducible in bounded time; it is covered by {@link
     * #cancelFalseReturnsTrueWhileBodyIsStillExecuting()} and {@link
     * #wakeDoesNotSubmitForAnAlreadyClaimedBackoff()} instead.
     */
    @Test
    @Timeout(120)
    public void queueSurvivesADuplicatedPollerChain() throws Exception {
        FakeMonitor monitor = newMonitor("DUPLICATE_CHAIN_QUEUE", 4);
        monitor.backlog.set(500);

        Method submitPoll = QueueMonitor.class.getDeclaredMethod("submitPoll");
        submitPoll.setAccessible(true);

        AtomicBoolean stop = new AtomicBoolean();
        Thread demand =
                daemon(
                        "demand",
                        () -> {
                            while (!stop.get()) {
                                monitor.pop(5, 50, TimeUnit.MILLISECONDS);
                            }
                        });
        demand.start();
        try {
            activePollers(monitor).set(1);
            submitPoll.invoke(monitor);
            submitPoll.invoke(monitor);
            Thread.sleep(500);
        } finally {
            stop.set(true);
            demand.join(10_000);
        }

        // Demand is gone, so both chains take the stop branch and release the same single slot.
        // Best effort only — a broken build never reaches 0, and the assertions below are the ones
        // that should report the defect.
        awaitPollersIdle(monitor, 20_000);
        assertTrue(
                monitor.getActivePollers() >= 0,
                "activePollers went negative: " + monitor.getActivePollers());

        monitor.backlog.set(200);
        assertTrue(
                popUntilNonEmpty(monitor, 15_000),
                "queue stopped delivering after a duplicated poller chain (activePollers="
                        + monitor.getActivePollers()
                        + ")");
    }

    /**
     * Concurrency guard, not a reproduction: it does not deterministically hit the cancel/wake
     * window, but it exercises the claim hand-off, clamp and collapse paths under contention.
     */
    @Test
    @Timeout(120)
    public void pollerSlotSurvivesWakeStorm() throws Exception {
        FakeMonitor monitor = newMonitor("STORM_QUEUE", 8);
        monitor.backlog.set(2_000);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger lowestObserved = new AtomicInteger(Integer.MAX_VALUE);
        List<Thread> threads = new ArrayList<>();

        // Producer: keep the queue hot so fetches stay productive and pushes keep arriving.
        threads.add(
                daemon(
                        "producer",
                        () -> {
                            while (!stop.get()) {
                                monitor.backlog.addAndGet(50);
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                            }
                        }));

        // Consumers: short waits, so liveDemand repeatedly drops back to zero between calls —
        // that is the moment at which a duplicated poller chain double-releases the slot.
        for (int i = 0; i < 4; i++) {
            threads.add(
                    daemon(
                            "consumer-" + i,
                            () -> {
                                while (!stop.get()) {
                                    monitor.pop(5, 20, TimeUnit.MILLISECONDS);
                                }
                            }));
        }

        // Notifiers: every push-wake is a chance for wakePoller() to race a firing backoff.
        for (int i = 0; i < 4; i++) {
            threads.add(
                    daemon(
                            "notifier-" + i,
                            () -> {
                                while (!stop.get()) {
                                    monitor.notifyMessageReady();
                                    LockSupport.parkNanos(50_000L);
                                }
                            }));
        }

        // Sampler. A negative count is absorbing, so sampling reliably catches it once it happens.
        threads.add(
                daemon(
                        "sampler",
                        () -> {
                            while (!stop.get()) {
                                lowestObserved.accumulateAndGet(
                                        monitor.getActivePollers(), Math::min);
                                LockSupport.parkNanos(10_000L);
                            }
                        }));

        threads.forEach(Thread::start);
        Thread.sleep(5_000);
        stop.set(true);
        for (Thread t : threads) {
            t.join(10_000);
        }

        assertTrue(
                lowestObserved.get() >= 0,
                "activePollers went negative (observed "
                        + lowestObserved.get()
                        + ") — the queue is permanently stalled from that point on");
        assertTrue(
                monitor.getActivePollers() >= 0,
                "activePollers left negative after the storm: " + monitor.getActivePollers());

        // Liveness is the assertion that matters: a stalled queue polls Redis no more, so no
        // amount of backlog can reach a consumer. Quiesce first so the pop below can only be
        // satisfied by a fresh fetch.
        quiesce(monitor);
        monitor.backlog.set(200);
        assertTrue(
                popUntilNonEmpty(monitor, 15_000),
                "queue stopped delivering after the wake storm (activePollers="
                        + monitor.getActivePollers()
                        + ", pollsTotal="
                        + monitor.getPollsTotal()
                        + ")");
    }

    // ------------------------------------------------------------------------------------------
    // 2. Targeted semantics.
    // ------------------------------------------------------------------------------------------

    /**
     * Pins the JDK contract the old guard got wrong: a mid-execution task is not done, {@code
     * cancel(false)} still returns true, and the runnable still completes. So reading that true as
     * "the body did not run" is unsound.
     */
    @Test
    @Timeout(30)
    public void cancelFalseReturnsTrueWhileBodyIsStillExecuting() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicBoolean bodyCompleted = new AtomicBoolean();
        try {
            ScheduledFuture<?> future =
                    scheduler.schedule(
                            () -> {
                                entered.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                }
                                bodyCompleted.set(true);
                                finished.countDown();
                            },
                            0,
                            TimeUnit.MILLISECONDS);

            assertTrue(entered.await(10, TimeUnit.SECONDS), "scheduled body never started");
            assertFalse(
                    future.isDone(), "an executing FutureTask must still report isDone() false");
            assertTrue(
                    future.cancel(false),
                    "cancel(false) on an executing task returns true — this is why it cannot be used"
                            + " as proof the body did not run");

            release.countDown();
            assertTrue(finished.await(10, TimeUnit.SECONDS), "body did not finish");
            assertTrue(
                    bodyCompleted.get(),
                    "a successfully 'cancelled' non-interrupting task still ran to completion");
        } finally {
            scheduler.shutdownNow();
        }
    }

    /**
     * The fix's invariant: the claim, not the cancel, decides who polls. Claiming from the test
     * stands in for the firing body having won — the case where the old code cancelled a running
     * body "successfully" and submitted anyway.
     */
    @Test
    @Timeout(60)
    public void wakeDoesNotSubmitForAnAlreadyClaimedBackoff() throws Exception {
        FakeMonitor monitor = newMonitor("CLAIM_QUEUE", 4);
        // Empty backlog: every poll is unproductive, so the poller keeps scheduling backoffs.
        monitor.backlog.set(0);

        AtomicBoolean stop = new AtomicBoolean();
        Thread demand =
                daemon(
                        "demand",
                        () -> {
                            while (!stop.get()) {
                                monitor.pop(1, 200, TimeUnit.MILLISECONDS);
                            }
                        });
        demand.start();
        try {
            Object claimed = claimAPendingPoll(monitor, 20_000);
            assertTrue(claimed != null, "no backoff was scheduled to claim");

            // The claimed registration is still published, so wakePoller() will see it and must
            // find it already taken.
            int submitsBefore = monitor.executor.executeCount.get();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
            while (System.nanoTime() < deadline) {
                monitor.notifyMessageReady();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            }

            assertEquals(
                    submitsBefore,
                    monitor.executor.executeCount.get(),
                    "wakePoller() submitted a poll for a backoff that was already claimed — that is"
                            + " the double-submit which drives activePollers negative");
        } finally {
            stop.set(true);
            demand.join(10_000);
        }
    }

    // ------------------------------------------------------------------------------------------
    // 3. Recovery from a corrupted slot count.
    // ------------------------------------------------------------------------------------------

    /**
     * A negative count must not be terminal: injecting {@code -1} has to heal and keep delivering.
     */
    @Test
    @Timeout(60)
    public void queueRecoversFromNegativePollerSlot() throws Exception {
        FakeMonitor monitor = newMonitor("RECOVERY_QUEUE", 4);
        monitor.backlog.set(100);

        // Warm up normally first, then poison the slot count. Quiescing first means the recovery
        // assertion below can only be satisfied by a fresh fetch, not by a stocked cache, and that
        // the injection is not racing a live chain that would clamp it back to 0 on its own.
        assertTrue(popUntilNonEmpty(monitor, 10_000), "queue did not deliver before the injection");
        quiesce(monitor);
        activePollers(monitor).set(-1);
        assertEquals(-1, monitor.getActivePollers());

        monitor.backlog.set(100);
        assertTrue(
                popUntilNonEmpty(monitor, 20_000),
                "queue never recovered from activePollers=-1 (still "
                        + monitor.getActivePollers()
                        + ")");
        assertTrue(
                monitor.getActivePollers() >= 0,
                "activePollers still negative: " + monitor.getActivePollers());
    }

    /** Releasing a slot that is not held must clamp at zero rather than underflow. */
    @Test
    @Timeout(30)
    public void releasingAnUnheldSlotClampsAtZero() throws Exception {
        FakeMonitor monitor = newMonitor("CLAMP_QUEUE", 2);
        Method release = QueueMonitor.class.getDeclaredMethod("releasePoller");
        release.setAccessible(true);

        assertEquals(0, monitor.getActivePollers());
        release.invoke(monitor);
        release.invoke(monitor);
        assertEquals(
                0,
                monitor.getActivePollers(),
                "an unheld release must clamp at 0, never drive the slot count negative");

        // And a queue whose slot was over-released still polls.
        monitor.backlog.set(20);
        assertTrue(
                popUntilNonEmpty(monitor, 10_000), "queue did not deliver after an over-release");
    }

    // ------------------------------------------------------------------------------------------
    // 4. Happy paths that must not regress.
    // ------------------------------------------------------------------------------------------

    /** A pop the inline fetch can satisfy must not hand anything to the poll executor. */
    @Test
    @Timeout(30)
    public void singleConsumerPopFetchesInlineWithoutExecutorHandOff() {
        FakeMonitor monitor = newMonitor("INLINE_QUEUE", 2);
        monitor.backlog.set(1);

        List<QueueMessage> messages = monitor.pop(1, 100, TimeUnit.MILLISECONDS);

        assertEquals(1, messages.size(), "inline fetch did not deliver the available message");
        assertEquals(
                0,
                monitor.executor.executeCount.get(),
                "single-consumer pop handed off to the poll executor instead of fetching inline");
        assertEquals(0, monitor.getActivePollers(), "inline fetch did not release the poller slot");
    }

    /**
     * A hot queue must still ramp helper pollers up to the demand-scaled cap, then collapse back.
     */
    @Test
    @Timeout(120)
    public void hotQueueRampsHelperPollersUpToTheDemandScaledCap() throws Exception {
        int maxPollers = staticInt("MAX_CONCURRENT_POLLERS");
        int demandPerPoller = staticInt("DEMAND_PER_POLLER");
        int maxCacheDepth = staticInt("MAX_CACHE_DEPTH");
        // Helpers are spawned against the cache-depth-capped demand, so this is the reachable cap.
        int expected =
                Math.max(
                        1,
                        Math.min(
                                maxPollers,
                                (maxCacheDepth + demandPerPoller - 1) / demandPerPoller));

        FakeMonitor monitor = newMonitor("HOT_QUEUE", maxPollers + 2);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger highestObserved = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();

        threads.add(
                daemon(
                        "producer",
                        () -> {
                            while (!stop.get()) {
                                monitor.backlog.addAndGet(200);
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                            }
                        }));
        // Enough concurrent demand that the cache cannot cover it, which is what warrants helpers.
        for (int i = 0; i < 8; i++) {
            threads.add(
                    daemon(
                            "consumer-" + i,
                            () -> {
                                while (!stop.get()) {
                                    monitor.pop(20, 200, TimeUnit.MILLISECONDS);
                                }
                            }));
        }
        threads.add(
                daemon(
                        "sampler",
                        () -> {
                            while (!stop.get()) {
                                highestObserved.accumulateAndGet(
                                        monitor.getActivePollers(), Math::max);
                                LockSupport.parkNanos(10_000L);
                            }
                        }));

        threads.forEach(Thread::start);
        try {
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && highestObserved.get() < expected) {
                Thread.sleep(50);
            }
        } finally {
            stop.set(true);
            for (Thread t : threads) {
                t.join(10_000);
            }
        }

        assertTrue(
                highestObserved.get() >= expected,
                "helper pollers did not ramp up: reached "
                        + highestObserved.get()
                        + ", expected at least "
                        + expected);
        assertTrue(
                highestObserved.get() <= maxPollers,
                "helper ramp-up exceeded MAX_CONCURRENT_POLLERS: " + highestObserved.get());

        // With demand gone and the queue drained, the chain collapses back and releases its slot.
        monitor.backlog.set(0);
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && monitor.getActivePollers() > 0) {
            Thread.sleep(50);
        }
        assertEquals(
                0,
                monitor.getActivePollers(),
                "pollers did not collapse after demand and backlog went away");
    }

    /**
     * Cached messages past their unack window are dropped, never delivered and never re-cached —
     * they are redeliverable from Redis, so returning them would double up.
     */
    @Test
    @Timeout(30)
    public void expiredCachedMessagesAreDroppedNotDelivered() {
        FakeMonitor monitor = newMonitor("EXPIRY_QUEUE", 2);
        // Negative unack time makes every fetched message expire before it can be handed out.
        monitor.setQueueUnackTime(-10_000);
        monitor.backlog.set(50);

        List<QueueMessage> messages = monitor.pop(5, 200, TimeUnit.MILLISECONDS);

        assertTrue(messages.isEmpty(), "expired messages must not be delivered: " + messages);
        assertTrue(
                monitor.getMessagesFetched() > 0,
                "test did not actually fetch anything, so it proves nothing");
        assertTrue(monitor.backlog.get() < 50, "fetches did not consume the backlog");
    }

    // ------------------------------------------------------------------------------------------
    // Fixture
    // ------------------------------------------------------------------------------------------

    private FakeMonitor newMonitor(String queueName, int poolSize) {
        CountingExecutor executor = new CountingExecutor(poolSize);
        executors.add(executor);
        return new FakeMonitor(queueName, executor);
    }

    private static Thread daemon(String name, Runnable body) {
        Thread t = new Thread(body, name);
        t.setDaemon(true);
        return t;
    }

    /**
     * Brings the queue to a standstill: no backlog, no chain, empty cache. Order matters — drain
     * the cache only after the chain stops, or it refills and a later liveness assertion passes
     * from the cache without the poller ever running.
     */
    private static void quiesce(FakeMonitor monitor) throws InterruptedException {
        monitor.backlog.set(0);
        assertTrue(
                awaitPollersIdle(monitor, 20_000),
                "poller chain never stopped with an empty backlog (activePollers="
                        + monitor.getActivePollers()
                        + ")");
        while (!monitor.pop(100, 1, TimeUnit.MILLISECONDS).isEmpty()) {
            // keep draining; the backlog is empty so nothing can refill it
        }
        // The drain pops registered demand and may have revived the chain; let it stop again.
        assertTrue(
                awaitPollersIdle(monitor, 20_000),
                "poller chain never stopped after draining the cache (activePollers="
                        + monitor.getActivePollers()
                        + ")");
        assertTrue(
                monitor.pop(1, 1, TimeUnit.MILLISECONDS).isEmpty(),
                "cache was not empty after quiescing");
    }

    /** Waits until no poller chain holds a slot, i.e. the queue has gone quiet. */
    private static boolean awaitPollersIdle(FakeMonitor monitor, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (monitor.getActivePollers() == 0) {
                return true;
            }
            Thread.sleep(20);
        }
        return monitor.getActivePollers() == 0;
    }

    private static boolean popUntilNonEmpty(FakeMonitor monitor, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!monitor.pop(1, 100, TimeUnit.MILLISECONDS).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Object> pendingRef(QueueMonitor monitor) throws Exception {
        Field field = QueueMonitor.class.getDeclaredField("pending");
        field.setAccessible(true);
        return (AtomicReference<Object>) field.get(monitor);
    }

    private static AtomicInteger activePollers(QueueMonitor monitor) throws Exception {
        Field field = QueueMonitor.class.getDeclaredField("activePollers");
        field.setAccessible(true);
        return (AtomicInteger) field.get(monitor);
    }

    private static int staticInt(String name) throws Exception {
        Field field = QueueMonitor.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(null);
    }

    /**
     * Wins the claim on a published backoff, standing in for the firing body. Null if the body beat
     * us to every one within the timeout.
     */
    private static Object claimAPendingPoll(QueueMonitor monitor, long timeoutMs) throws Exception {
        AtomicReference<Object> pending = pendingRef(monitor);
        Method claim = null;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Object p = pending.get();
            if (p != null) {
                if (claim == null) {
                    claim = p.getClass().getDeclaredMethod("claim");
                    claim.setAccessible(true);
                }
                if (Boolean.TRUE.equals(claim.invoke(p))) {
                    return p;
                }
            }
            LockSupport.parkNanos(100_000L);
        }
        return null;
    }

    /** {@link QueueMonitor} over an in-memory backlog counter — no Redis, no Docker. */
    private static final class FakeMonitor extends QueueMonitor {

        final AtomicInteger backlog = new AtomicInteger();
        final AtomicLong idSeq = new AtomicLong();
        final CountingExecutor executor;

        FakeMonitor(String queueName, CountingExecutor executor) {
            super(queueName, executor);
            this.executor = executor;
        }

        @Override
        protected List<String> pollMessages(double now, double maxTime, int batchSize) {
            int taken = 0;
            while (taken < batchSize) {
                int current = backlog.get();
                if (current <= 0) {
                    break;
                }
                if (backlog.compareAndSet(current, current - 1)) {
                    taken++;
                }
            }
            if (taken == 0) {
                return Collections.emptyList();
            }
            List<String> response = new ArrayList<>(taken * 2);
            String score = Double.toString(now);
            for (int i = 0; i < taken; i++) {
                response.add("msg-" + idSeq.incrementAndGet());
                response.add(score);
            }
            return response;
        }

        @Override
        protected long queueSize() {
            return Math.max(0, backlog.get());
        }

        @Override
        protected long readySize(double now) {
            return Math.max(0, backlog.get());
        }

        @Override
        protected double lowestScore() {
            return backlog.get() > 0 ? 0 : -1;
        }
    }

    /** Counts hand-offs. {@code submitPoll()} is the only caller of {@code execute}. */
    private static final class CountingExecutor extends ThreadPoolExecutor {

        final AtomicInteger executeCount = new AtomicInteger();

        CountingExecutor(int poolSize) {
            super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            setThreadFactory(
                    runnable -> {
                        Thread t = new Thread(runnable, "test-poller");
                        t.setDaemon(true);
                        return t;
                    });
        }

        @Override
        public void execute(Runnable command) {
            // Count only accepted hand-offs: a rejection propagates and QueueMonitor releases the
            // slot, exactly as in production.
            super.execute(command);
            executeCount.incrementAndGet();
        }
    }
}
