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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.orkes.conductor.mq.QueueMessage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The backoff timer fires while {@code wakePoller()} cancels it. {@code FutureTask.cancel(false)}
 * returns {@code true} for a task that is already running, so both must not restart the poller.
 */
class QueueMonitorWakeRaceTest {

    private final ManualExecutor pollExecutor = new ManualExecutor();
    private final FakeScheduler scheduler = new FakeScheduler();
    private final FakeMonitor monitor = new FakeMonitor(pollExecutor, scheduler);

    @Test
    void timerAndWakeRestartThePollerOnlyOnce() throws Exception {
        AtomicInteger activePollers = activePollers(monitor);

        // A consumer waits for messages; it starts the poller.
        Thread consumer = new Thread(() -> monitor.pop(1, 10_000, TimeUnit.MILLISECONDS));
        consumer.start();
        pollExecutor.awaitTasks(1);
        assertEquals(1, activePollers.get());

        // The poller finds the queue empty and pauses on a backoff timer.
        pollExecutor.runNext();
        FakeFuture timer = scheduler.lastScheduled();

        // A push wakes the poller. The timer fires the moment the wake touches it.
        timer.fireOnFirstTouch = true;
        Thread.sleep(20); // past the push-wake coalescing window
        monitor.notifyMessageReady();
        timer.finish();

        int restarts = pollExecutor.submitted.get();

        // The consumer leaves; every poll loop stops.
        consumer.interrupt();
        consumer.join();
        pollExecutor.runAll();
        int pollersAfterStop = activePollers.get();

        // A new message arrives.
        monitor.pending.add("m1");
        List<QueueMessage> got = monitor.pop(1, 100, TimeUnit.MILLISECONDS);

        assertAll(
                () -> assertEquals(1, restarts, "poller restarts after one pause"),
                () -> assertEquals(0, pollersAfterStop, "activePollers after all pollers stopped"),
                () -> assertEquals(1, got.size(), "messages delivered after the race"));
    }

    static final class FakeMonitor extends QueueMonitor {
        final ConcurrentLinkedQueue<String> pending = new ConcurrentLinkedQueue<>();

        FakeMonitor(ExecutorService executor, ScheduledExecutorService scheduler) {
            super("wake-race-queue", executor, scheduler);
        }

        @Override
        protected List<String> pollMessages(double now, double maxTime, int batchSize) {
            List<String> out = new ArrayList<>();
            String id;
            while (out.size() / 2 < batchSize && (id = pending.poll()) != null) {
                out.add(id);
                out.add(String.valueOf(now));
            }
            return out;
        }

        @Override
        protected long queueSize() {
            return pending.size();
        }

        @Override
        protected long readySize(double now) {
            return pending.size();
        }

        @Override
        protected double lowestScore() {
            return -1;
        }
    }

    /** Runs poll cycles only when the test says so. Blocks the timer thread inside execute(). */
    static final class ManualExecutor extends AbstractExecutorService {
        final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        final AtomicInteger submitted = new AtomicInteger();
        volatile Thread timerThread;
        final CountDownLatch timerInExecute = new CountDownLatch(1);
        final CountDownLatch releaseTimer = new CountDownLatch(1);

        @Override
        public void execute(Runnable command) {
            submitted.incrementAndGet();
            tasks.add(command);
            if (Thread.currentThread() == timerThread) {
                timerInExecute.countDown();
                await(releaseTimer);
            }
        }

        void awaitTasks(int n) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000;
            while (tasks.size() < n && System.currentTimeMillis() < deadline) {
                Thread.sleep(1);
            }
            assertEquals(n, tasks.size());
            submitted.set(0);
        }

        void runNext() {
            tasks.remove().run();
        }

        void runAll() {
            Runnable r;
            while ((r = tasks.poll()) != null) {
                r.run();
            }
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    /** Holds scheduled tasks; the test decides when they fire. */
    final class FakeScheduler extends AbstractExecutorService implements ScheduledExecutorService {
        private volatile FakeFuture last;

        FakeFuture lastScheduled() {
            return last;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            last = new FakeFuture(command);
            return last;
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    /**
     * Same state rules as {@link FutureTask}: the state stays NEW while the task runs, so {@code
     * cancel(false)} on a running task returns {@code true} and does not stop it.
     */
    final class FakeFuture implements ScheduledFuture<Object> {
        private static final int NEW = 0, CANCELLED = 1, DONE = 2;

        private final Runnable task;
        private final AtomicInteger state = new AtomicInteger(NEW);
        private final CountDownLatch timerStarted = new CountDownLatch(1);
        private Thread timerThread;
        volatile boolean fireOnFirstTouch;

        FakeFuture(Runnable task) {
            this.task = task;
        }

        /** Fires the timer on its own thread and waits until it is inside submitPoll() or done. */
        private synchronized void touched() {
            if (!fireOnFirstTouch || timerThread != null) {
                return;
            }
            timerThread = new Thread(this::run, "fake-backoff-timer");
            pollExecutor.timerThread = timerThread;
            timerThread.start();
            awaitAny(pollExecutor.timerInExecute, timerStarted);
        }

        private void run() {
            if (state.get() == NEW) {
                task.run();
                state.compareAndSet(NEW, DONE);
            }
            timerStarted.countDown();
        }

        void finish() throws InterruptedException {
            pollExecutor.releaseTimer.countDown();
            if (timerThread != null) {
                timerThread.join();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            touched();
            return state.compareAndSet(NEW, CANCELLED);
        }

        @Override
        public boolean isDone() {
            touched();
            return state.get() != NEW;
        }

        @Override
        public boolean isCancelled() {
            return state.get() == CANCELLED;
        }

        @Override
        public Object get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitAny(CountDownLatch a, CountDownLatch b) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (a.getCount() > 0 && b.getCount() > 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timed out");
            }
            Thread.onSpinWait();
        }
    }

    private static AtomicInteger activePollers(QueueMonitor m) throws Exception {
        Field f = QueueMonitor.class.getDeclaredField("activePollers");
        f.setAccessible(true);
        return (AtomicInteger) f.get(m);
    }
}
