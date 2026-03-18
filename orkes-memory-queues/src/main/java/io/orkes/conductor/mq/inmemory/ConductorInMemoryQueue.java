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

import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;

import com.google.common.util.concurrent.Uninterruptibles;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of {@link ConductorQueue} backed by a {@link ConcurrentSkipListSet}
 * (replacing Redis ZSET) and {@link ConcurrentHashMap} maps (replacing Redis Hash). Supports
 * durable disk persistence via either a {@link WriteAheadLog} (fast, O(1) appends) or a legacy
 * {@link QueueStatePersistence} (full snapshot per mutation).
 */
@Slf4j
public class ConductorInMemoryQueue implements ConductorQueue {

    private final String queueName;
    private final Clock clock;

    // ZSET equivalent: sorted by (score, messageId)
    private final ConcurrentSkipListSet<ScoredMessage> sortedMessages;

    // ZSCORE equivalent: messageId -> score for O(1) lookups
    private final ConcurrentHashMap<String, Double> scoreIndex;

    // Hash equivalent: messageId -> payload
    private final ConcurrentHashMap<String, String> payloads;

    // Lock for atomic pop (fetch eligible + re-score)
    private final ReentrantLock popLock = new ReentrantLock();

    private volatile int queueUnackTime = 30_000;

    private final QueueStatePersistence persistence;
    private final WriteAheadLog wal;

    public ConductorInMemoryQueue(String queueName, QueueStatePersistence persistence) {
        this(queueName, persistence, null, null);
    }

    public ConductorInMemoryQueue(String queueName, WriteAheadLog wal) {
        this(queueName, null, wal, null);
    }

    public ConductorInMemoryQueue(
            String queueName,
            QueueStatePersistence persistence,
            QueueStatePersistence.QueueState initialState) {
        this(queueName, persistence, null, initialState);
    }

    public ConductorInMemoryQueue(
            String queueName,
            QueueStatePersistence persistence,
            WriteAheadLog wal,
            QueueStatePersistence.QueueState initialState) {
        this.queueName = queueName;
        this.clock = Clock.systemDefaultZone();
        this.sortedMessages = new ConcurrentSkipListSet<>();
        this.scoreIndex = new ConcurrentHashMap<>();
        this.payloads = new ConcurrentHashMap<>();
        this.persistence = persistence;
        this.wal = wal;

        if (initialState != null) {
            hydrate(initialState);
        }

        log.info("ConductorInMemoryQueue started serving {}", queueName);
    }

    private void hydrate(QueueStatePersistence.QueueState state) {
        this.queueUnackTime = state.getQueueUnackTime();
        if (state.getMessages() != null) {
            for (QueueStatePersistence.MessageEntry entry : state.getMessages()) {
                ScoredMessage sm = new ScoredMessage(entry.getScore(), entry.getId());
                sortedMessages.add(sm);
                scoreIndex.put(entry.getId(), entry.getScore());
                if (entry.getPayload() != null) {
                    payloads.put(entry.getId(), entry.getPayload());
                }
            }
        }
        log.info("Hydrated queue {} with {} messages from disk", queueName, scoreIndex.size());
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        if (count <= 0) {
            return new ArrayList<>();
        }

        List<QueueMessage> messages = doPop(count);
        if (messages.isEmpty() && waitTime > 0) {
            Uninterruptibles.sleepUninterruptibly(waitTime, timeUnit);
            messages = doPop(count);
        }
        return messages;
    }

    /**
     * Atomic pop: find messages with score <= now, re-score them to now + queueUnackTime.
     * Equivalent to the pop_batch.lua script.
     */
    private List<QueueMessage> doPop(int count) {
        List<QueueMessage> result = new ArrayList<>();
        long now = clock.millis();
        List<WriteAheadLog.WalEntry> walEntries = wal != null ? new ArrayList<>() : null;

        popLock.lock();
        try {
            Iterator<ScoredMessage> iterator = sortedMessages.iterator();
            List<ScoredMessage> toRemove = new ArrayList<>();
            List<ScoredMessage> toAdd = new ArrayList<>();

            while (iterator.hasNext() && result.size() < count) {
                ScoredMessage sm = iterator.next();
                if (sm.getScore() > now) {
                    break;
                }

                toRemove.add(sm);

                double newScore = now + queueUnackTime;
                ScoredMessage newSm = new ScoredMessage(newScore, sm.getMessageId());
                toAdd.add(newSm);
                scoreIndex.put(sm.getMessageId(), newScore);

                if (walEntries != null) {
                    walEntries.add(WriteAheadLog.WalEntry.rescore(sm.getMessageId(), newScore));
                }

                int priority = (int) ((sm.getScore() % 1.0) * 100);

                String payload = payloads.get(sm.getMessageId());
                QueueMessage msg =
                        new QueueMessage(
                                sm.getMessageId(), payload, (long) sm.getScore(), priority);
                result.add(msg);
            }

            sortedMessages.removeAll(toRemove);
            sortedMessages.addAll(toAdd);
        } finally {
            popLock.unlock();
        }

        if (!result.isEmpty()) {
            if (walEntries != null) {
                appendWalEntries(walEntries);
            } else {
                notifyPersistence();
            }
        }

        return result;
    }

    @Override
    public boolean ack(String messageId) {
        Double score = scoreIndex.remove(messageId);
        if (score == null) {
            return false;
        }
        sortedMessages.remove(new ScoredMessage(score, messageId));
        payloads.remove(messageId);
        if (wal != null) {
            appendWalEntry(WriteAheadLog.WalEntry.remove(messageId));
        } else {
            notifyPersistence();
        }
        return true;
    }

    @Override
    public void push(List<QueueMessage> messages) {
        long now = clock.millis();
        List<WriteAheadLog.WalEntry> walEntries = wal != null ? new ArrayList<>() : null;
        for (QueueMessage msg : messages) {
            double score = getScore(now, msg);
            String messageId = msg.getId();

            // Remove old entry if exists (ZADD overwrites score)
            Double oldScore = scoreIndex.put(messageId, score);
            if (oldScore != null) {
                sortedMessages.remove(new ScoredMessage(oldScore, messageId));
            }
            sortedMessages.add(new ScoredMessage(score, messageId));

            String payload = null;
            if (StringUtils.isNotBlank(msg.getPayload())) {
                payloads.put(messageId, msg.getPayload());
                payload = msg.getPayload();
            }

            if (walEntries != null) {
                walEntries.add(WriteAheadLog.WalEntry.push(messageId, score, payload));
            }
        }
        if (walEntries != null) {
            appendWalEntries(walEntries);
        } else {
            notifyPersistence();
        }
    }

    @Override
    public boolean setUnacktimeout(String messageId, long unackTimeout) {
        Double oldScore = scoreIndex.get(messageId);
        if (oldScore == null) {
            return false; // XX semantics: only update, don't add
        }
        double newScore = clock.millis() + unackTimeout;
        sortedMessages.remove(new ScoredMessage(oldScore, messageId));
        sortedMessages.add(new ScoredMessage(newScore, messageId));
        scoreIndex.put(messageId, newScore);
        if (wal != null) {
            appendWalEntry(WriteAheadLog.WalEntry.rescore(messageId, newScore));
        } else {
            notifyPersistence();
        }
        return true;
    }

    @Override
    public boolean exists(String messageId) {
        return scoreIndex.containsKey(messageId);
    }

    @Override
    public void remove(String messageId) {
        Double score = scoreIndex.remove(messageId);
        if (score != null) {
            sortedMessages.remove(new ScoredMessage(score, messageId));
            payloads.remove(messageId);
            if (wal != null) {
                appendWalEntry(WriteAheadLog.WalEntry.remove(messageId));
            } else {
                notifyPersistence();
            }
        }
    }

    @Override
    public QueueMessage get(String messageId) {
        Double score = scoreIndex.get(messageId);
        if (score == null) {
            return null;
        }
        int priority = (int) ((score % 1.0) * 100);
        String payload = payloads.get(messageId);
        return new QueueMessage(messageId, payload, score.longValue(), priority);
    }

    @Override
    public void flush() {
        sortedMessages.clear();
        scoreIndex.clear();
        payloads.clear();
        if (wal != null) {
            wal.delete(queueName);
        } else if (persistence != null) {
            persistence.delete(queueName);
        }
    }

    @Override
    public long size() {
        return scoreIndex.size();
    }

    @Override
    public int getQueueUnackTime() {
        return queueUnackTime;
    }

    @Override
    public void setQueueUnackTime(int queueUnackTime) {
        this.queueUnackTime = queueUnackTime;
    }

    @Override
    public String getShardName() {
        return null;
    }

    /** Snapshot the current state for persistence. */
    QueueStatePersistence.QueueState snapshot() {
        List<QueueStatePersistence.MessageEntry> entries = new ArrayList<>();
        for (ScoredMessage sm : sortedMessages) {
            String payload = payloads.get(sm.getMessageId());
            entries.add(
                    new QueueStatePersistence.MessageEntry(
                            sm.getMessageId(), sm.getScore(), payload));
        }
        return new QueueStatePersistence.QueueState(queueName, queueUnackTime, entries);
    }

    /** Synchronously persist the current queue state to disk (legacy full-snapshot path). */
    private void notifyPersistence() {
        if (persistence != null) {
            persistence.persistNow(queueName, snapshot());
        }
    }

    /** Append a single WAL entry and trigger compaction if threshold is reached. */
    private void appendWalEntry(WriteAheadLog.WalEntry entry) {
        int count = wal.appendEntry(queueName, entry);
        maybeCompact(count);
    }

    /** Append multiple WAL entries in one flush and trigger compaction if needed. */
    private void appendWalEntries(List<WriteAheadLog.WalEntry> entries) {
        int count = wal.appendEntries(queueName, entries);
        maybeCompact(count);
    }

    private void maybeCompact(int walEntryCount) {
        if (walEntryCount >= wal.getCompactThreshold()) {
            wal.compact(queueName, snapshot());
        }
    }
}
