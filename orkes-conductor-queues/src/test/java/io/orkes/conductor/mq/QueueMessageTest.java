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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link QueueMessage}. */
public class QueueMessageTest {

    @Test
    public void testDefaultConstructor() {
        QueueMessage msg = new QueueMessage("id1", "payload1");
        assertEquals("id1", msg.getId());
        assertEquals("payload1", msg.getPayload());
        assertEquals(0, msg.getTimeout());
        assertEquals(100, msg.getPriority());
    }

    @Test
    public void testConstructorWithTimeout() {
        QueueMessage msg = new QueueMessage("id2", "payload2", 5000);
        assertEquals("id2", msg.getId());
        assertEquals("payload2", msg.getPayload());
        assertEquals(5000, msg.getTimeout());
        assertEquals(100, msg.getPriority());
    }

    @Test
    public void testFullConstructor() {
        QueueMessage msg = new QueueMessage("id3", "payload3", 3000, 5);
        assertEquals("id3", msg.getId());
        assertEquals("payload3", msg.getPayload());
        assertEquals(3000, msg.getTimeout());
        assertEquals(5, msg.getPriority());
    }

    @Test
    public void testSetters() {
        QueueMessage msg = new QueueMessage("id", "payload");

        msg.setId("newId");
        assertEquals("newId", msg.getId());

        msg.setPayload("newPayload");
        assertEquals("newPayload", msg.getPayload());

        msg.setPriority(42);
        assertEquals(42, msg.getPriority());

        msg.setExpiry(123456789L);
        assertEquals(123456789L, msg.getExpiry());
    }

    @Test
    public void testSetTimeoutMillis() {
        QueueMessage msg = new QueueMessage("id", "payload");
        msg.setTimeout(5000);
        assertEquals(5000, msg.getTimeout());
    }

    @Test
    public void testSetTimeoutWithTimeUnit() {
        QueueMessage msg = new QueueMessage("id", "payload");
        msg.setTimeout(2, TimeUnit.SECONDS);
        assertEquals(2000, msg.getTimeout());
    }

    @Test
    public void testEqualsSameObject() {
        QueueMessage msg = new QueueMessage("id1", "payload");
        assertEquals(msg, msg);
    }

    @Test
    public void testEqualsEqualId() {
        QueueMessage msg1 = new QueueMessage("id1", "payload1");
        QueueMessage msg2 = new QueueMessage("id1", "payload2");
        assertEquals(msg1, msg2);
    }

    @Test
    public void testEqualsDifferentId() {
        QueueMessage msg1 = new QueueMessage("id1", "payload");
        QueueMessage msg2 = new QueueMessage("id2", "payload");
        assertNotEquals(msg1, msg2);
    }

    @Test
    public void testEqualsNull() {
        QueueMessage msg = new QueueMessage("id1", "payload");
        assertNotEquals(null, msg);
    }

    @Test
    public void testEqualsDifferentClass() {
        QueueMessage msg = new QueueMessage("id1", "payload");
        assertNotEquals("id1", msg);
    }

    @Test
    public void testHashCode() {
        QueueMessage msg1 = new QueueMessage("id1", "payload1");
        QueueMessage msg2 = new QueueMessage("id1", "payload2");
        assertEquals(msg1.hashCode(), msg2.hashCode());

        QueueMessage msg3 = new QueueMessage("id2", "payload");
        assertNotEquals(msg1.hashCode(), msg3.hashCode());
    }

    @Test
    public void testGetDelay() {
        // Message with 5 second timeout
        QueueMessage msg = new QueueMessage("id", "payload", 5000);
        long delay = msg.getDelay(TimeUnit.MILLISECONDS);
        // Should be approximately 5000ms (minus a tiny amount for execution time)
        assertTrue(delay > 4000 && delay <= 5000, "Delay was: " + delay);

        // Message with 0 timeout should have non-positive delay
        QueueMessage msg2 = new QueueMessage("id2", "payload");
        long delay2 = msg2.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(delay2 <= 0, "Delay was: " + delay2);
    }

    @Test
    public void testGetDelayConversion() {
        QueueMessage msg = new QueueMessage("id", "payload", 5000);
        long delaySeconds = msg.getDelay(TimeUnit.SECONDS);
        assertTrue(delaySeconds >= 4 && delaySeconds <= 5, "Delay was: " + delaySeconds);
    }

    @Test
    public void testCompareToLessThan() {
        QueueMessage msg1 = new QueueMessage("id1", "payload", 1000);
        QueueMessage msg2 = new QueueMessage("id2", "payload", 5000);
        assertTrue(msg1.compareTo(msg2) < 0);
    }

    @Test
    public void testCompareToGreaterThan() {
        QueueMessage msg1 = new QueueMessage("id1", "payload", 5000);
        QueueMessage msg2 = new QueueMessage("id2", "payload", 1000);
        assertTrue(msg1.compareTo(msg2) > 0);
    }

    @Test
    public void testCompareToEqual() {
        QueueMessage msg1 = new QueueMessage("id1", "payload", 0);
        QueueMessage msg2 = new QueueMessage("id2", "payload", 0);
        assertEquals(0, msg1.compareTo(msg2));
    }

    @Test
    public void testNullPayload() {
        QueueMessage msg = new QueueMessage("id", null);
        assertNull(msg.getPayload());
    }

    @Test
    public void testExpiry() {
        QueueMessage msg = new QueueMessage("id", "payload");
        assertEquals(0, msg.getExpiry());
        msg.setExpiry(System.currentTimeMillis() + 30000);
        assertTrue(msg.getExpiry() > 0);
    }
}
