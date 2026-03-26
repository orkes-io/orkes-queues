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
package io.orkes.conductor.mq;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/** Represents a message in a conductor queue with support for priority and delayed delivery. */
public class QueueMessage implements Delayed {

    private String id;

    private String payload;

    /** Time in millisecond for delayed pop */
    private long timeout;

    /** Priority - 0 being the highest priority */
    private int priority;

    private long expiry;

    private long time; // Epoch time when the message is ready to come out

    /**
     * Creates a message with default timeout of 0 and default priority of 100.
     *
     * @param id the message id
     * @param payload the message payload
     */
    public QueueMessage(String id, String payload) {
        this(id, payload, 0, 100);
    }

    /**
     * Creates a message with the given timeout and default priority of 100.
     *
     * @param id the message id
     * @param payload the message payload
     * @param timeout time in milliseconds after which message should be available
     */
    public QueueMessage(String id, String payload, long timeout) {
        this(id, payload, timeout, 100);
    }

    /**
     * Creates a message with all fields specified.
     *
     * @param id Message Id
     * @param payload Payload
     * @param timeout Time in millis after which message should be available. for time delayed
     *     behavior
     * @param priority Priority
     */
    public QueueMessage(String id, String payload, long timeout, int priority) {
        this.id = id;
        this.payload = payload;
        this.timeout = timeout;
        this.priority = priority;
        this.time = System.currentTimeMillis() + this.timeout;
    }

    /**
     * Returns the message id.
     *
     * @return the message id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the message id.
     *
     * @param id the message id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Returns the message payload.
     *
     * @return the message payload
     */
    public String getPayload() {
        return payload;
    }

    /**
     * Sets the message payload.
     *
     * @param payload the message payload
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     * Returns the timeout in milliseconds.
     *
     * @return the timeout in milliseconds
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout in milliseconds and recalculates the ready time.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
        this.time = System.currentTimeMillis() + this.timeout;
    }

    /**
     * Sets the timeout with the given time unit and recalculates the ready time.
     *
     * @param timeout the timeout value
     * @param timeUnit the time unit for the timeout
     */
    public void setTimeout(long timeout, TimeUnit timeUnit) {
        // FIXME: FIX IN THE MAIN BRANCH
        this.timeout = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
        this.time = System.currentTimeMillis() + this.timeout;
    }

    /**
     * Returns the message priority.
     *
     * @return the priority value
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Sets the message priority.
     *
     * @param priority the priority value
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Returns the message expiry time in epoch milliseconds.
     *
     * @return the expiry time
     */
    public long getExpiry() {
        return expiry;
    }

    /**
     * Sets the message expiry time in epoch milliseconds.
     *
     * @param expiry the expiry time
     */
    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueMessage message = (QueueMessage) o;
        return Objects.equals(id, message.id);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /** {@inheritDoc} */
    @Override
    public long getDelay(TimeUnit unit) {
        long diff = time - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(Delayed obj) {
        if (this.time < ((QueueMessage) obj).time) {
            return -1;
        }
        if (this.time > ((QueueMessage) obj).time) {
            return 1;
        }
        return 0;
    }
}
