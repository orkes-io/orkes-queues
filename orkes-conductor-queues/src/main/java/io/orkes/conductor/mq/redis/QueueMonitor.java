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

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.orkes.conductor.mq.QueueMessage;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract queue monitor that handles message polling and caching from Redis-backed queues.
 *
 * <p><b>Decoupled poller design (#4).</b> Consumer threads never issue Redis calls directly. A
 * single self-scheduling poller per queue keeps the in-memory {@link #peekedMessages} cache filled;
 * {@link #pop} only reads that cache and blocks (up to the caller's {@code waitTime}) waiting for
 * it to fill. This decouples Redis poll traffic from the number of consumer threads (no stampede at
 * 100+ pollers), reduces idle polling via adaptive backoff, and re-polls <em>during</em> the
 * consumer wait so messages that arrive mid-wait are delivered promptly.
 *
 * <p>The poller body runs on the per-queue {@code executorService} passed to the constructor (so we
 * do not create one dedicated thread per queue — Conductor has thousands of queues). A single
 * shared static daemon scheduler is used <em>only</em> to time the adaptive backoff between empty
 * polls.
 */
@Slf4j
public abstract class QueueMonitor {

    /**
     * Shared, process-wide timer used <em>only</em> to schedule the next poll after a backoff. The
     * actual Redis poll always runs on the per-queue {@link #executorService}; this scheduler does
     * no Redis work and therefore stays tiny regardless of how many queues exist.
     */
    private static final ScheduledExecutorService BACKOFF_SCHEDULER =
            Executors.newScheduledThreadPool(
                    Math.max(2, Runtime.getRuntime().availableProcessors() / 4),
                    runnable -> {
                        Thread t = new Thread(runnable, "queue-monitor-backoff");
                        t.setDaemon(true);
                        return t;
                    });

    private final Clock clock;

    private final LinkedBlockingQueue<QueueMessage> peekedMessages;

    private final ExecutorService executorService;

    /**
     * Sum of the {@code count} argument of every currently in-flight {@link #pop} call. Incremented
     * on entry and decremented in a {@code finally}, so it is self-correcting and never leaks even
     * when a pop times out. The poller fetches at most this many messages, which keeps the cache
     * shallow and demand-bounded: we never pull more into memory than consumers are actively
     * waiting for (every fetched message is rescored invisible for {@code queueUnackTime}, so
     * over-fetching would strand messages until the unack window expires).
     */
    private final AtomicInteger liveDemand = new AtomicInteger(0);

    /** True while a poll for this queue is in flight (single in-flight poll guard). */
    private final AtomicInteger activePollers = new AtomicInteger(0);

    /**
     * Maximum number of concurrent in-flight pollers per queue. Concurrent pollers are
     * duplicate-safe because Redis executes each {@code evalsha} atomically and serially, so two
     * pollers fetch disjoint sets of due messages. More pollers parallelize the Redis round-trip,
     * which is what recovers throughput on a single hot/saturated queue (one serialized poller is
     * RTT-bound). The actual concurrency is also capped by the per-queue executor's pool size.
     */
    private static final int MAX_CONCURRENT_POLLERS =
            Integer.getInteger("orkes.queue.maxPollers", 8);

    /**
     * Live demand per poller. The desired poller count grows with outstanding demand up to {@link
     * #MAX_CONCURRENT_POLLERS}, so a sparse/idle queue runs a single poller (cheap, few evalsha)
     * while a hot queue ramps up to parallelize fetches.
     */
    private static final int DEMAND_PER_POLLER =
            Integer.getInteger("orkes.queue.demandPerPoller", 32);

    @Getter @Setter private int queueUnackTime = 30_000;

    private final int MAX_POLL_COUNT = 1000;

    /**
     * Upper bound on consecutive Redis fetches in a single poll cycle before yielding the executor
     * thread back. Keeps a single very hot queue from monopolizing the shared poll executor.
     */
    private static final int MAX_LOOP_FETCHES = 64;

    /** Backoff bounds for the idle poller (milliseconds). */
    private static final long MIN_BACKOFF_MS = 1;

    /** Backoff cap used when no caller has supplied a poll {@code waitTime} yet. */
    private static final long DEFAULT_MAX_BACKOFF_MS =
            Integer.getInteger("orkes.queue.maxPollBackoffMs", 50);

    /** Absolute ceiling on the idle backoff regardless of a (possibly huge) caller waitTime. */
    private static final long BACKOFF_CEILING_MS = 1_000;

    /**
     * Push-wake coalescing window (ms). A push only wakes the poller if the previous poll was more
     * than this long ago, so a burst of pushes is gathered into a single batched poll. This bounds
     * push-driven polling to ~one poll per window per queue while keeping delivery latency ≈ this
     * window for a hot queue (and ≈ a poll round-trip for a sparse one, where pushes are further
     * apart than the window).
     */
    private static final long BATCH_WINDOW_MS = Integer.getInteger("orkes.queue.batchWindowMs", 5);

    /**
     * Once a {@code pop} holds at least one message, it waits at most this long for each additional
     * message to fill the requested batch, instead of the full remaining {@code waitTime}. Bounds
     * the latency a held message accrues while the call gathers a batch.
     */
    private static final long BATCH_GATHER_NANOS =
            Integer.getInteger("orkes.queue.batchGatherMs", 2) * 1_000_000L;

    /** {@code nanoTime} of the most recent Redis poll, for push-wake coalescing. */
    private volatile long lastPollNanos;

    private volatile long currentBackoffMs = MIN_BACKOFF_MS;

    /** Handle to the pending backoff-scheduled poll, so a new {@link #pop} can preempt it. */
    private volatile ScheduledFuture<?> backoffFuture;

    /**
     * Wall-clock ({@code nanoTime}) at which the next poll is expected to run. Used to decide
     * whether a newly-arriving {@link #pop} needs to preempt a long idle backoff.
     */
    private volatile long nextPollDueNanos;

    /**
     * The most recent {@code pop} waitTime (ms). The idle backoff grows up to this value: a caller
     * willing to wait {@code waitTime} for messages does not benefit from the poller hitting an
     * empty queue more often than that, and on the canonical one-worker-per-queue topology polling
     * faster would be a pure (and large) waste. Hot queues are unaffected — a productive fetch
     * resets the backoff to {@link #MIN_BACKOFF_MS}.
     */
    private volatile long pollWaitMillis = 0;

    /**
     * How long helper pollers linger after the last productive fetch before collapsing back to a
     * single poller. Lets a hot but <em>bursty</em> queue (briefly empty between arrivals) retain
     * its parallel pollers, while a genuinely idle queue still collapses to one poller.
     */
    private static final long HELPER_LINGER_MS =
            Integer.getInteger("orkes.queue.helperLingerMs", 50);

    private volatile long lastProductiveMs = 0;

    // Lightweight poll metrics (cheap atomics). Exposed for observability/tuning: an empty poll is
    // one that hit Redis but found nothing due. A high empty ratio under load indicates the poller
    // is polling faster than messages arrive (back off / batch up).
    private final AtomicLong pollsTotal = new AtomicLong();
    private final AtomicLong pollsEmpty = new AtomicLong();
    private final AtomicLong messagesFetched = new AtomicLong();

    /** Total Redis polls (evalsha) issued by this queue's poller. */
    public long getPollsTotal() {
        return pollsTotal.get();
    }

    /** Polls that returned no messages (Redis had nothing due). */
    public long getPollsEmpty() {
        return pollsEmpty.get();
    }

    /** Total messages fetched from Redis into the cache. */
    public long getMessagesFetched() {
        return messagesFetched.get();
    }

    private final boolean cached;

    private final String queueName;

    /**
     * Creates a new queue monitor.
     *
     * @param queueName the name of the queue to monitor
     * @param executorService the executor service for async polling
     */
    public QueueMonitor(String queueName, ExecutorService executorService) {
        this.clock = Clock.systemDefaultZone();
        this.peekedMessages = new LinkedBlockingQueue<>();
        this.executorService = executorService;
        this.cached = !queueName.contains("RATE_LIMITED_WORKFLOW");
        this.queueName = queueName;
    }

    /**
     * Pops messages from the queue, using caching for non-rate-limited queues.
     *
     * <p>Consumers only read the in-memory cache here — the dedicated poller is responsible for all
     * Redis traffic. We register demand, kick the poller if it is idle, then run a wait-loop: drain
     * whatever is cached, and while we still want more and time remains, block on the cache for the
     * next message. This guarantees batch completeness (up to {@code count} messages in one call on
     * a stocked queue) while still returning early enough to honour {@code waitTime}.
     *
     * @param count number of messages to pop
     * @param waitTime time to wait if no messages are available
     * @param timeUnit time unit for waitTime
     * @return list of popped messages
     */
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        if (!cached) {
            return popStrict(count);
        }

        List<QueueMessage> messages = new ArrayList<>(count);
        pollWaitMillis = timeUnit.toMillis(waitTime);
        liveDemand.addAndGet(count);
        try {
            final long deadlineNanos = System.nanoTime() + timeUnit.toNanos(waitTime);

            // Make sure the poller is running to satisfy the demand we just registered. Only
            // preempt
            // a pending idle backoff if its next poll would land AFTER our deadline — otherwise the
            // scheduled poll already covers us. This keeps the canonical one-worker-per-queue case
            // (poll lands within waitTime) from waking on every pop, while still serving a short
            // wait that is stuck behind a long backoff.
            ensurePollerRunning();
            if (peekedMessages.isEmpty() && nextPollDueNanos - deadlineNanos > 0) {
                wakePoller();
            }

            // Drain whatever is already cached (cheap, non-blocking).
            drainInto(messages, count);

            // Wait-loop. We long-poll up to `waitTime` for the FIRST message, but once we have at
            // least one we only briefly "gather" the rest of an arriving batch (BATCH_GATHER_NANOS)
            // — otherwise a pop(count) on a sparse queue would block the whole waitTime waiting to
            // fill `count`, adding that delay to the latency of the message it already holds. With
            // event-driven wake this is what keeps delivery latency at ~the poll round-trip while a
            // stocked queue still returns a full batch (its messages are all immediately
            // drainable).
            while (messages.size() < count) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                long waitNanos =
                        messages.isEmpty() ? remaining : Math.min(remaining, BATCH_GATHER_NANOS);
                QueueMessage message = peekedMessages.poll(waitNanos, TimeUnit.NANOSECONDS);
                if (message == null) {
                    break; // first-message wait expired, or no more arrived within the gather
                    // window
                }
                long now = clock.millis();
                if (now > message.getExpiry()) {
                    // Stale entry that sat in the cache past its unack window — drop it (do NOT add
                    // it back, or it would be eligible for redelivery from Redis too and double
                    // up).
                    continue;
                }
                messages.add(message);
                // Opportunistically take any more that are immediately available.
                drainInto(messages, count - messages.size());
            }
            return messages;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return messages;
        } finally {
            liveDemand.addAndGet(-count);
        }
    }

    /**
     * Non-blocking drain that appends up to {@code want} non-expired messages from the cache into
     * {@code dest}. Expired entries encountered are discarded (never returned, never re-cached), so
     * a stale message that sat past its unack window cannot be delivered.
     */
    private void drainInto(List<QueueMessage> dest, int want) {
        if (want <= 0) {
            return;
        }
        long now = clock.millis();
        int added = 0;
        QueueMessage m;
        while (added < want && (m = peekedMessages.poll()) != null) {
            if (now > m.getExpiry()) {
                continue; // drop expired, keep draining
            }
            dest.add(m);
            added++;
        }
    }

    /**
     * Polls messages from the underlying store.
     *
     * @param now the current time as a double
     * @param maxTime the maximum score for messages to poll
     * @param batchSize the maximum number of messages to poll
     * @return list of alternating message ids and scores, or null if no messages
     */
    protected abstract List<String> pollMessages(double now, double maxTime, int batchSize);

    /**
     * Returns the current size of the queue.
     *
     * @return the number of messages in the queue
     */
    protected abstract long queueSize();

    /**
     * Idle backoff cap: grow up to the caller's poll waitTime (so we never poll an empty queue more
     * often than a consumer is willing to wait), bounded by a hard ceiling and floored at the
     * default when no waitTime is known yet.
     */
    private long maxBackoffMs() {
        long w = pollWaitMillis;
        if (w <= 0) {
            w = DEFAULT_MAX_BACKOFF_MS;
        }
        return Math.max(MIN_BACKOFF_MS, Math.min(w, BACKOFF_CEILING_MS));
    }

    /**
     * Starts the dedicated poller for this queue if it is not already running. Cheap no-op when a
     * poll is already in flight. Called by every {@link #pop} so a queue that went idle (poller
     * stopped) is revived as soon as new demand appears.
     */
    private void ensurePollerRunning() {
        if (liveDemand.get() <= 0) {
            return;
        }
        // Ensure at least one poller. Additional (helper) pollers are spawned only from within a
        // *productive* poll cycle (see maybeSpawnHelper), so a queue that is merely being polled
        // hard while empty stays at a single poller and a single backoff cadence.
        if (activePollers.compareAndSet(0, 1)) {
            submitPoll();
        }
    }

    /**
     * Cap on concurrent pollers for the given demand. Helpers ramp up only when fetches are
     * productive, so this bound applies to a genuinely hot queue, not an empty one being polled by
     * many waiting consumers.
     */
    private static int desiredPollers(int demand) {
        int byDemand = (demand + DEMAND_PER_POLLER - 1) / DEMAND_PER_POLLER;
        return Math.max(1, Math.min(MAX_CONCURRENT_POLLERS, byDemand));
    }

    /**
     * After a productive fetch, add one helper poller if the queue is hot enough to warrant it
     * (cache still cannot cover outstanding demand) and we are below the demand-scaled cap. Ramps
     * up gradually — one helper per productive cycle — which is fast under sustained load and
     * avoids spawning helpers for a brief burst.
     */
    private void maybeSpawnHelper() {
        int demand = liveDemand.get();
        if (demand - peekedMessages.size() <= 0) {
            return;
        }
        int cur = activePollers.get();
        if (cur < desiredPollers(demand) && activePollers.compareAndSet(cur, cur + 1)) {
            submitPoll();
        }
    }

    /**
     * Submits one poll cycle to the per-queue executor. Caller has already reserved a poller slot
     * by incrementing {@link #activePollers}.
     */
    private void submitPoll() {
        try {
            executorService.execute(this::pollCycle);
        } catch (RejectedExecutionException rejected) {
            // Executor is shutting down/saturated — release the slot so a later pop() can retry.
            activePollers.decrementAndGet();
        }
    }

    /**
     * One poll cycle. Runs on the per-queue executor. Fetches up to the current live demand,
     * populates the cache, then decides whether to reschedule:
     *
     * <ul>
     *   <li>fetched &gt; 0: reschedule immediately (delay 0) and reset backoff — keep the cache
     *       fed.
     *   <li>fetched == 0: back off (exponential up to the cap) and reschedule.
     *   <li>no demand: stop. The guard is cleared; a subsequent {@link #pop} restarts the poller.
     *       We re-check demand after clearing the flag to close the stop/restart race.
     * </ul>
     */
    private void pollCycle() {
        boolean reschedule = false;
        long delay = 0;
        boolean redisEmpty = false;
        try {
            // Tight inner loop: while a fetch is productive and demand still outstrips what is
            // already cached, keep fetching on THIS thread rather than round-tripping through the
            // executor for every batch. Redis serializes evalsha anyway, so a single in-flight
            // poller is the natural unit of work; staying on-thread removes scheduling latency and
            // is what keeps a hot/saturated queue's throughput up. Bounded by MAX_LOOP_FETCHES so
            // one queue cannot monopolize the shared executor thread.
            boolean fetchedAny = false;
            boolean drained = false; // Redis had no more due messages (empty or partial batch)
            boolean cacheFull = false; // cache already covers outstanding demand
            for (int i = 0; i < MAX_LOOP_FETCHES; i++) {
                int unfilled = liveDemand.get() - peekedMessages.size();
                if (unfilled <= 0) {
                    cacheFull = true;
                    break; // cache already covers outstanding demand — do not over-fetch
                }
                int batch = Math.min(MAX_POLL_COUNT, unfilled);
                int fetched = fetchIntoCache(batch);
                if (fetched <= 0) {
                    redisEmpty = true;
                    drained = true;
                    break; // Redis had nothing due
                }
                fetchedAny = true;
                lastProductiveMs = clock.millis();
                if (fetched < batch) {
                    // Got everything that was due (Redis returned fewer than we asked for). The
                    // queue is drained, so stop here rather than issuing another (empty) poll just
                    // to discover that — the next message arrives via push-wake or the fallback
                    // backoff.
                    drained = true;
                    break;
                }
            }

            // Keep the poller alive as long as consumers are waiting (liveDemand > 0). The poller
            // only stops when demand drops to zero; the next pop() revives it. This avoids a stall
            // where the poller stops while a consumer is still blocked needing more messages.
            if (liveDemand.get() > 0) {
                reschedule = true;
                if (drained) {
                    // Nothing more is due right now: back off exponentially up to the cap so an
                    // idle/sparse queue costs few evalsha calls. A freshly pushed message wakes the
                    // poller immediately (coalesced push-wake), so delivery latency does not hinge
                    // on this backoff — it is only the fallback cadence for missed/cross-process
                    // notifications.
                    delay = currentBackoffMs;
                    currentBackoffMs = Math.min(maxBackoffMs(), currentBackoffMs * 2);
                    if (fetchedAny) {
                        maybeSpawnHelper();
                    }
                } else if (cacheFull) {
                    // Cache already covers outstanding demand. Pause one tick (avoid a CPU-burning
                    // re-dispatch spin) and keep backoff reset so we stay responsive.
                    currentBackoffMs = MIN_BACKOFF_MS;
                    delay = MIN_BACKOFF_MS;
                } else {
                    // Hit the loop cap with full batches: more is available, keep going now.
                    currentBackoffMs = MIN_BACKOFF_MS;
                    delay = 0;
                    maybeSpawnHelper();
                }
            }
        } catch (Throwable t) {
            log.warn("poll cycle failed for {}: {}", queueName, t.getMessage(), t);
            if (liveDemand.get() > 0) {
                reschedule = true;
                delay = currentBackoffMs;
            }
        } finally {
            // Continue this poller chain unless: demand dropped to zero, OR the queue is empty and
            // we have more than one poller (collapse the helpers back to a single poller so an
            // empty-but-hammered queue costs only one backoff cadence), OR we are over the
            // demand-scaled cap.
            int demand = liveDemand.get();
            boolean collapseExtra =
                    redisEmpty
                            && activePollers.get() > 1
                            && (clock.millis() - lastProductiveMs) > HELPER_LINGER_MS;
            boolean keepThisPoller =
                    reschedule
                            && demand > 0
                            && !collapseExtra
                            && activePollers.get() <= desiredPollers(demand);
            if (keepThisPoller) {
                reschedulePoll(delay);
            } else {
                // This poller stops: release its slot, then re-check demand to avoid a lost wakeup
                // where a pop() registered demand between our checks and the decrement.
                activePollers.decrementAndGet();
                ensurePollerRunning();
            }
        }
    }

    /**
     * Reschedules the next poll cycle on the per-queue executor after {@code delayMs}. Does not
     * change the active-poller count — this is the same chain continuing.
     */
    private void reschedulePoll(long delayMs) {
        if (delayMs <= 0) {
            nextPollDueNanos = System.nanoTime();
            submitPoll();
            return;
        }
        nextPollDueNanos = System.nanoTime() + delayMs * 1_000_000L;
        try {
            backoffFuture =
                    BACKOFF_SCHEDULER.schedule(
                            () -> {
                                backoffFuture = null;
                                submitPoll();
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rejected) {
            activePollers.decrementAndGet();
        }
    }

    /**
     * Brings a backed-off poller forward when a new {@link #pop} arrives. The idle backoff can grow
     * up to {@code waitTime}; without this, a message that arrives (or a consumer that starts
     * waiting with a shorter timeout) could sit until a long backoff sleep elapses. We cancel the
     * pending sleep and poll immediately, continuing the same poller chain.
     */
    private void wakePoller() {
        ScheduledFuture<?> f = backoffFuture;
        if (f != null && !f.isDone() && f.cancel(false)) {
            // Bring the next poll forward to now. Do NOT reset the backoff to the floor: under a
            // steady push-wake stream that would restart the exponential ramp on every message and
            // produce a burst of empty polls between arrivals. The backoff is the fallback cadence;
            // a productive fetch (the hot-queue path) is what resets it.
            backoffFuture = null;
            nextPollDueNanos = System.nanoTime();
            submitPoll();
        }
    }

    /**
     * Notifies the poller that a message which is due now has just been pushed to this queue, so it
     * can fetch it immediately instead of waiting out its idle backoff. This is the event-driven
     * path that drives queue wait time toward zero: when {@code push} and the consumer share a JVM,
     * delivery latency becomes a poll round-trip rather than up to {@code waitTime}.
     *
     * <p>No-op when nothing is waiting to consume (no consumer is blocked, so there is no urgency —
     * the next {@code pop} starts polling) or on the non-cached path. The poller's backoff remains
     * the correctness fallback: a missed notification simply delays delivery to the next scheduled
     * poll, it never drops a message.
     */
    public void notifyMessageReady() {
        if (!cached || liveDemand.get() <= 0) {
            return;
        }
        ensurePollerRunning();
        // Coalesce: wake at most once per batch window. If we polled within the last
        // BATCH_WINDOW_MS, a burst of pushes is gathered by the next (already-imminent) poll rather
        // than each forcing its own. Sparse queues (pushes further apart than the window) wake per
        // message → ~poll-round-trip latency; hot queues coalesce → one batched poll per window,
        // latency ≈ window. This is what keeps a hot shared queue from polling per push.
        if (System.nanoTime() - lastPollNanos > BATCH_WINDOW_MS * 1_000_000L) {
            wakePoller();
        }
    }

    /**
     * Fetches up to {@code batch} due messages from Redis and appends them to the cache. Returns
     * the number fetched; a value &lt; {@code batch} means Redis had nothing more due (drained).
     */
    private int fetchIntoCache(int batch) {
        if (batch <= 0) {
            return 0;
        }
        lastPollNanos = System.nanoTime();
        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double maxTime = now + queueUnackTime;
        long messageExpiry = (long) now + queueUnackTime;
        List<String> response = pollMessages(now, maxTime, batch);
        pollsTotal.incrementAndGet();
        if (response == null || response.isEmpty()) {
            pollsEmpty.incrementAndGet();
            return 0;
        }
        int fetched = 0;
        for (int i = 0; i < response.size(); i += 2) {
            String id = response.get(i);
            String scoreString = response.get(i + 1);
            int priority = decodePriority(scoreString);
            QueueMessage message = new QueueMessage(id, "", 0, priority);
            message.setExpiry(messageExpiry);
            peekedMessages.add(message);
            fetched++;
        }
        messagesFetched.addAndGet(fetched);
        return fetched;
    }

    /**
     * Decodes the priority that {@link io.orkes.conductor.mq.ConductorQueue#getScore} encoded into
     * the fractional part of the score. Uses primitive arithmetic rather than {@code BigDecimal} —
     * the score is already a double, so there is no precision to recover, and this runs once per
     * polled message on the hot path.
     */
    private static int decodePriority(String scoreString) {
        double score = Double.parseDouble(scoreString);
        return (int) ((score - Math.floor(score)) * 100);
    }

    private List<QueueMessage> popStrict(int count) {
        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double maxTime = now + queueUnackTime;
        long messageExpiry = (long) now + (queueUnackTime);
        List<String> response = pollMessages(now, maxTime, count);
        List<QueueMessage> result = new ArrayList<>();
        if (response == null) {
            return result;
        }
        for (int i = 0; i < response.size(); i += 2) {
            long timeout = 0;
            String id = response.get(i);
            String scoreString = response.get(i + 1);
            int priority = decodePriority(scoreString);
            QueueMessage message = new QueueMessage(id, "", timeout, priority);
            message.setExpiry(messageExpiry);
            result.add(message);
        }
        return result;
    }
}
