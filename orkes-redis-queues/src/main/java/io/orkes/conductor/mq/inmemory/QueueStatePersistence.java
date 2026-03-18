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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Disk persistence for in-memory queues. Supports two modes:
 *
 * <ul>
 *   <li><b>Synchronous</b> ({@link #persistNow}) — blocks the caller until the state is durable
 *       on disk. Use this for push, pop, ack, and any mutation where durability is required.
 *   <li><b>Asynchronous</b> ({@link #markDirty}) — coalesced, fire-and-forget writes on a
 *       background thread. Suitable only for best-effort / non-critical persistence.
 * </ul>
 *
 * Files are written atomically via tmp+rename.
 */
@Slf4j
public class QueueStatePersistence {

    private final Path dataDir;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;
    private final Set<String> pendingWrites = ConcurrentHashMap.newKeySet();

    public QueueStatePersistence(Path dataDir) {
        this.dataDir = dataDir;
        this.objectMapper = new ObjectMapper();
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "queue-persistence");
            t.setDaemon(true);
            return t;
        });
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create queue data directory: " + dataDir, e);
        }
    }

    /**
     * Synchronously persist the queue state to disk. Blocks until the write is durable.
     * This is the preferred method for all mutating operations where durability is critical.
     */
    public void persistNow(String queueName, QueueState state) {
        writeToFile(queueName, state);
    }

    /**
     * Mark a queue as dirty. The state supplier will be called on the persistence thread to get a
     * point-in-time snapshot. Writes are coalesced — only one write per queue can be pending.
     *
     * <p><b>Warning</b>: This is best-effort async persistence. For durable writes, use
     * {@link #persistNow} instead.
     */
    public void markDirty(String queueName, Supplier<QueueState> stateSupplier) {
        if (pendingWrites.add(queueName)) {
            executor.submit(() -> {
                try {
                    pendingWrites.remove(queueName);
                    QueueState state = stateSupplier.get();
                    writeToFile(queueName, state);
                } catch (Exception e) {
                    log.error("Failed to persist queue {}", queueName, e);
                }
            });
        }
    }

    /** Load a single queue's state from disk. Returns null if no persisted state exists. */
    public QueueState load(String queueName) {
        Path file = dataDir.resolve(sanitize(queueName) + ".json");
        if (!Files.exists(file)) {
            return null;
        }
        try {
            byte[] data = Files.readAllBytes(file);
            return objectMapper.readValue(data, QueueState.class);
        } catch (IOException e) {
            log.error("Failed to load queue state from {}", file, e);
            return null;
        }
    }

    /** Load all persisted queue states from the data directory. */
    public Map<String, QueueState> loadAll() {
        Map<String, QueueState> states = new HashMap<>();
        File[] files = dataDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));
        if (files == null) {
            return states;
        }
        for (File file : files) {
            try {
                byte[] data = Files.readAllBytes(file.toPath());
                QueueState state = objectMapper.readValue(data, QueueState.class);
                if (state != null && state.getQueueName() != null) {
                    states.put(state.getQueueName(), state);
                }
            } catch (IOException e) {
                log.error("Failed to load queue state from {}", file, e);
            }
        }
        return states;
    }

    /** Synchronously remove the persisted state file for a queue. */
    public void delete(String queueName) {
        try {
            Path file = dataDir.resolve(sanitize(queueName) + ".json");
            Files.deleteIfExists(file);
        } catch (IOException e) {
            log.error("Failed to delete queue state file for {}", queueName, e);
        }
    }

    /** Synchronously write state to disk. Used for testing and shutdown. */
    public void writeSync(String queueName, QueueState state) {
        writeToFile(queueName, state);
    }

    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void writeToFile(String queueName, QueueState state) {
        Path tmpFile = dataDir.resolve(sanitize(queueName) + ".json.tmp");
        Path targetFile = dataDir.resolve(sanitize(queueName) + ".json");
        try {
            byte[] data = objectMapper.writeValueAsBytes(state);
            Files.write(tmpFile, data);
            Files.move(tmpFile, targetFile, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to write queue state to {}", targetFile, e);
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException ignored) {
            }
        }
    }

    static String sanitize(String queueName) {
        return queueName.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /** Represents the persisted state of a single queue. */
    public static class QueueState {
        private String queueName;
        private int queueUnackTime;
        private List<MessageEntry> messages;

        public QueueState() {}

        public QueueState(String queueName, int queueUnackTime, List<MessageEntry> messages) {
            this.queueName = queueName;
            this.queueUnackTime = queueUnackTime;
            this.messages = messages;
        }

        public String getQueueName() {
            return queueName;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        public int getQueueUnackTime() {
            return queueUnackTime;
        }

        public void setQueueUnackTime(int queueUnackTime) {
            this.queueUnackTime = queueUnackTime;
        }

        public List<MessageEntry> getMessages() {
            return messages;
        }

        public void setMessages(List<MessageEntry> messages) {
            this.messages = messages;
        }
    }

    /** A single message entry for persistence. */
    public static class MessageEntry {
        private String id;
        private double score;
        private String payload;

        public MessageEntry() {}

        public MessageEntry(String id, double score, String payload) {
            this.id = id;
            this.score = score;
            this.payload = payload;
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
    }
}
