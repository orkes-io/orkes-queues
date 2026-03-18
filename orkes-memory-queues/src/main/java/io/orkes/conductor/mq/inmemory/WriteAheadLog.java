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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;

/**
 * Append-only write-ahead log for durable queue persistence with O(1) per-operation cost.
 *
 * <p>Instead of rewriting the full queue state on every mutation, each operation appends a small
 * delta record to a log file. Periodic compaction rewrites the full snapshot and truncates the log.
 *
 * <p>File layout per queue:
 *
 * <ul>
 *   <li>{queue}.json — latest compacted snapshot
 *   <li>{queue}.wal — append-only log of operations since last snapshot
 * </ul>
 */
@Slf4j
public class WriteAheadLog {

    private final Path dataDir;
    private final ObjectMapper objectMapper;
    private final ObjectWriter walEntryWriter;
    private final int compactThreshold;
    private final ConcurrentHashMap<String, AtomicInteger> walCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, OutputStream> walStreams = new ConcurrentHashMap<>();

    public WriteAheadLog(Path dataDir) {
        this(dataDir, 1000);
    }

    public WriteAheadLog(Path dataDir, int compactThreshold) {
        this.dataDir = dataDir;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.walEntryWriter = this.objectMapper.writerFor(WalEntry.class);
        this.compactThreshold = compactThreshold;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create queue data directory: " + dataDir, e);
        }
    }

    /**
     * Append a single WAL entry. This is an O(1) operation — only the delta is written. Uses a
     * persistent buffered stream per queue to avoid open/close overhead. Returns the number of
     * entries in the WAL (for compaction decisions).
     */
    public int appendEntry(String queueName, WalEntry entry) {
        try {
            byte[] line = walEntryWriter.writeValueAsBytes(entry);
            OutputStream os = getOrCreateStream(queueName);
            synchronized (os) {
                os.write(line);
                os.write('\n');
                os.flush();
            }
        } catch (IOException e) {
            log.error("Failed to append WAL entry for queue {}", queueName, e);
            closeStream(queueName);
        }
        return walCounters.computeIfAbsent(queueName, k -> new AtomicInteger(0)).incrementAndGet();
    }

    /**
     * Append multiple WAL entries atomically (single flush). More efficient for batch operations.
     * Returns the updated entry count.
     */
    public int appendEntries(String queueName, List<WalEntry> entries) {
        if (entries.isEmpty())
            return walCounters.computeIfAbsent(queueName, k -> new AtomicInteger(0)).get();
        try {
            OutputStream os = getOrCreateStream(queueName);
            synchronized (os) {
                for (WalEntry entry : entries) {
                    byte[] line = walEntryWriter.writeValueAsBytes(entry);
                    os.write(line);
                    os.write('\n');
                }
                os.flush();
            }
        } catch (IOException e) {
            log.error("Failed to append WAL entries for queue {}", queueName, e);
            closeStream(queueName);
        }
        return walCounters
                .computeIfAbsent(queueName, k -> new AtomicInteger(0))
                .addAndGet(entries.size());
    }

    private OutputStream getOrCreateStream(String queueName) throws IOException {
        OutputStream existing = walStreams.get(queueName);
        if (existing != null) return existing;
        Path walFile = walPath(queueName);
        OutputStream newStream =
                new BufferedOutputStream(
                        Files.newOutputStream(
                                walFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                        8192);
        OutputStream prev = walStreams.putIfAbsent(queueName, newStream);
        if (prev != null) {
            newStream.close();
            return prev;
        }
        return newStream;
    }

    private void closeStream(String queueName) {
        OutputStream os = walStreams.remove(queueName);
        if (os != null) {
            try {
                os.close();
            } catch (IOException ignored) {
            }
        }
    }

    /** Write a full snapshot and clear the WAL. Called during compaction. */
    public void compact(String queueName, QueueStatePersistence.QueueState state) {
        writeSnapshot(queueName, state);
        // Close stream, truncate WAL, and reset counter
        closeStream(queueName);
        try {
            Files.deleteIfExists(walPath(queueName));
        } catch (IOException e) {
            log.error("Failed to truncate WAL for queue {}", queueName, e);
        }
        walCounters.computeIfAbsent(queueName, k -> new AtomicInteger(0)).set(0);
    }

    /** Load a queue's state by reading the snapshot and replaying the WAL on top of it. */
    public QueueStatePersistence.QueueState load(String queueName) {
        QueueStatePersistence.QueueState state = loadSnapshot(queueName);
        List<WalEntry> entries = loadWalEntries(queueName);
        if (state == null && entries.isEmpty()) {
            return null;
        }
        if (state == null) {
            state = new QueueStatePersistence.QueueState(queueName, 30_000, new ArrayList<>());
        }
        return replay(state, entries);
    }

    /** Load all queues by scanning for snapshot files and WAL files. */
    public java.util.Map<String, QueueStatePersistence.QueueState> loadAll() {
        java.util.Map<String, QueueStatePersistence.QueueState> states = new java.util.HashMap<>();
        // Collect all queue names from both .json and .wal files
        java.util.Set<String> queueNames = new java.util.HashSet<>();
        File[] files = dataDir.toFile().listFiles();
        if (files == null) return states;
        for (File f : files) {
            String name = f.getName();
            if (name.endsWith(".json") && !name.endsWith(".json.tmp")) {
                queueNames.add(name.substring(0, name.length() - 5));
            } else if (name.endsWith(".wal")) {
                queueNames.add(name.substring(0, name.length() - 4));
            }
        }
        for (String sanitizedName : queueNames) {
            // We need the actual queue name, which is stored in the snapshot
            QueueStatePersistence.QueueState snapshot = loadSnapshotByFilename(sanitizedName);
            List<WalEntry> entries = loadWalEntriesByFilename(sanitizedName);
            QueueStatePersistence.QueueState state;
            if (snapshot != null) {
                state = replay(snapshot, entries);
            } else if (!entries.isEmpty()) {
                // Try to get the queue name from the first entry
                state =
                        new QueueStatePersistence.QueueState(
                                sanitizedName, 30_000, new ArrayList<>());
                state = replay(state, entries);
            } else {
                continue;
            }
            if (state != null && state.getQueueName() != null) {
                states.put(state.getQueueName(), state);
            }
        }
        return states;
    }

    /** Delete all files for a queue. */
    public void delete(String queueName) {
        closeStream(queueName);
        try {
            Files.deleteIfExists(snapshotPath(queueName));
            Files.deleteIfExists(walPath(queueName));
        } catch (IOException e) {
            log.error("Failed to delete queue files for {}", queueName, e);
        }
        walCounters.remove(queueName);
    }

    public int getCompactThreshold() {
        return compactThreshold;
    }

    // ── Internal ────────────────────────────────────────────────────────

    private void writeSnapshot(String queueName, QueueStatePersistence.QueueState state) {
        String sanitized = QueueStatePersistence.sanitize(queueName);
        Path tmpFile = dataDir.resolve(sanitized + ".json.tmp");
        Path targetFile = dataDir.resolve(sanitized + ".json");
        try {
            byte[] data = objectMapper.writeValueAsBytes(state);
            Files.write(tmpFile, data);
            Files.move(
                    tmpFile,
                    targetFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to write snapshot for queue {}", queueName, e);
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException ignored) {
            }
        }
    }

    private QueueStatePersistence.QueueState loadSnapshot(String queueName) {
        Path file = snapshotPath(queueName);
        return readSnapshotFile(file);
    }

    private QueueStatePersistence.QueueState loadSnapshotByFilename(String sanitizedName) {
        Path file = dataDir.resolve(sanitizedName + ".json");
        return readSnapshotFile(file);
    }

    private QueueStatePersistence.QueueState readSnapshotFile(Path file) {
        if (!Files.exists(file)) return null;
        try {
            byte[] data = Files.readAllBytes(file);
            return objectMapper.readValue(data, QueueStatePersistence.QueueState.class);
        } catch (IOException e) {
            log.error("Failed to load snapshot from {}", file, e);
            return null;
        }
    }

    private List<WalEntry> loadWalEntries(String queueName) {
        Path walFile = walPath(queueName);
        return readWalFile(walFile);
    }

    private List<WalEntry> loadWalEntriesByFilename(String sanitizedName) {
        Path walFile = dataDir.resolve(sanitizedName + ".wal");
        return readWalFile(walFile);
    }

    private List<WalEntry> readWalFile(Path walFile) {
        List<WalEntry> entries = new ArrayList<>();
        if (!Files.exists(walFile)) return entries;
        try (BufferedReader reader = Files.newBufferedReader(walFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    entries.add(objectMapper.readValue(line, WalEntry.class));
                } catch (JsonProcessingException e) {
                    log.warn("Skipping corrupted WAL entry: {}", line);
                }
            }
        } catch (IOException e) {
            log.error("Failed to read WAL file {}", walFile, e);
        }
        return entries;
    }

    /** Replay WAL entries on top of a base state. */
    private QueueStatePersistence.QueueState replay(
            QueueStatePersistence.QueueState state, List<WalEntry> entries) {
        if (entries.isEmpty()) return state;

        // Build mutable index from snapshot
        java.util.Map<String, QueueStatePersistence.MessageEntry> index =
                new java.util.LinkedHashMap<>();
        if (state.getMessages() != null) {
            for (QueueStatePersistence.MessageEntry me : state.getMessages()) {
                index.put(me.getId(), me);
            }
        }

        for (WalEntry entry : entries) {
            switch (entry.getOp()) {
                case PUSH:
                    index.put(
                            entry.getId(),
                            new QueueStatePersistence.MessageEntry(
                                    entry.getId(), entry.getScore(), entry.getPayload()));
                    break;
                case RESCORE:
                    QueueStatePersistence.MessageEntry existing = index.get(entry.getId());
                    if (existing != null) {
                        index.put(
                                entry.getId(),
                                new QueueStatePersistence.MessageEntry(
                                        entry.getId(), entry.getScore(), existing.getPayload()));
                    }
                    break;
                case REMOVE:
                    index.remove(entry.getId());
                    break;
                case SET_UNACK_TIME:
                    if (entry.getQueueUnackTime() > 0) {
                        state.setQueueUnackTime(entry.getQueueUnackTime());
                    }
                    break;
                case FLUSH:
                    index.clear();
                    break;
            }
        }

        state.setMessages(new ArrayList<>(index.values()));
        return state;
    }

    private Path snapshotPath(String queueName) {
        return dataDir.resolve(QueueStatePersistence.sanitize(queueName) + ".json");
    }

    private Path walPath(String queueName) {
        return dataDir.resolve(QueueStatePersistence.sanitize(queueName) + ".wal");
    }

    /** Represents a single operation in the write-ahead log. */
    public static class WalEntry {
        private Op op;
        private String id;
        private double score;
        private String payload;
        private int queueUnackTime;

        public WalEntry() {}

        public WalEntry(Op op, String id, double score, String payload) {
            this.op = op;
            this.id = id;
            this.score = score;
            this.payload = payload;
        }

        public static WalEntry push(String id, double score, String payload) {
            return new WalEntry(Op.PUSH, id, score, payload);
        }

        public static WalEntry rescore(String id, double newScore) {
            return new WalEntry(Op.RESCORE, id, newScore, null);
        }

        public static WalEntry remove(String id) {
            return new WalEntry(Op.REMOVE, id, 0, null);
        }

        public static WalEntry flush() {
            return new WalEntry(Op.FLUSH, null, 0, null);
        }

        public Op getOp() {
            return op;
        }

        public void setOp(Op op) {
            this.op = op;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public int getQueueUnackTime() {
            return queueUnackTime;
        }

        public void setQueueUnackTime(int queueUnackTime) {
            this.queueUnackTime = queueUnackTime;
        }
    }

    public enum Op {
        PUSH,
        RESCORE,
        REMOVE,
        FLUSH,
        SET_UNACK_TIME
    }
}
