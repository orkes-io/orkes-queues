/*
 * Copyright 2022 Orkes, Inc.
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
import java.util.concurrent.TimeUnit;

public class QueueMessage {

    private String id;

    private String payload;

    /** Time in millisecond for delayed pop */
    private long timeout;

    /** Priority - 0 being the highest priority */
    private int priority;

    private long expiry;

    public QueueMessage(String id, String payload) {
        this(id, payload, 0, 100);
    }

    public QueueMessage(String id, String payload, long timeout) {
        this(id, payload, timeout, 100);
    }

    public QueueMessage(String id, String payload, long timeout, int priority) {
        this.id = id;
        this.payload = payload;
        this.timeout = timeout;
        this.priority = priority;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void setTimeout(long timeout, TimeUnit timeUnit) {
        this.timeout = timeUnit.convert(timeout, TimeUnit.MILLISECONDS);
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getExpiry() {
        return expiry;
    }

    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueMessage message = (QueueMessage) o;
        return Objects.equals(id, message.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
