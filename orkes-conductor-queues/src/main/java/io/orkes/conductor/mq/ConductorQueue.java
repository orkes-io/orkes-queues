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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Interface for conductor queue operations with support for priority and delayed delivery. */
public interface ConductorQueue {

    /** Constant for priority scaling. */
    BigDecimal HUNDRED = new BigDecimal(100);

    /** Math context with precision of 20 for score calculations. */
    MathContext PRECISION_MC = new MathContext(20);

    /**
     * Returns the name of this queue.
     *
     * @return the queue name
     */
    String getName();

    /**
     * Pops messages from the queue.
     *
     * @param count number of messages to pop
     * @param waitTime time to wait if no messages are available
     * @param timeUnit time unit for waitTime
     * @return list of popped messages
     */
    List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit);

    /**
     * Acknowledges a message, removing it from the queue.
     *
     * @param messageId the message id to acknowledge
     * @return true if the message was successfully acknowledged
     */
    boolean ack(String messageId);

    /**
     * Pushes messages onto the queue.
     *
     * @param messages the messages to push
     */
    void push(List<QueueMessage> messages);

    /**
     * Sets the unack timeout for a message.
     *
     * @param messageId the message id
     * @param unackTimeout the new unack timeout in milliseconds
     * @return true if the timeout was successfully updated
     */
    boolean setUnacktimeout(String messageId, long unackTimeout);

    /**
     * Sets the unack timeout for a message only if the new delivery time is sooner than the
     * currently scheduled one — i.e. never extends the existing timeout. Implementations that
     * support an atomic "update-if-lower" operation (e.g. {@code ZADD XX LT} in Redis) should
     * override this method. The default falls back to an unconditional {@link
     * #setUnacktimeout(String, long)}.
     *
     * @param messageId the message id
     * @param unackTimeout the new unack timeout in milliseconds
     * @return true if the timeout was updated
     */
    default boolean setUnacktimeoutIfShorter(String messageId, long unackTimeout) {
        return setUnacktimeout(messageId, unackTimeout);
    }

    /**
     * Checks if a message exists in the queue.
     *
     * @param messageId the message id to check
     * @return true if the message exists
     */
    boolean exists(String messageId);

    /**
     * Removes a message from the queue.
     *
     * @param messageId the message id to remove
     */
    void remove(String messageId);

    /**
     * Gets a message by its id.
     *
     * @param messageId the message id
     * @return the message, or null if not found
     */
    QueueMessage get(String messageId);

    /** Removes all messages from the queue. */
    void flush();

    /**
     * Returns the number of messages in the queue.
     *
     * @return the queue size
     */
    long size();

    /**
     * Returns the unack timeout for this queue in milliseconds.
     *
     * @return the queue unack time
     */
    int getQueueUnackTime();

    /**
     * Sets the unack timeout for this queue.
     *
     * @param queueUnackTime the unack time in milliseconds
     */
    void setQueueUnackTime(int queueUnackTime);

    /**
     * Returns the shard name for this queue.
     *
     * @return the shard name
     */
    String getShardName();

    /**
     * Calculates the score for a message based on its timeout and priority.
     *
     * @param now the current time in milliseconds
     * @param msg the queue message
     * @return the calculated score
     */
    default double getScore(long now, QueueMessage msg) {
        double score = 0;
        if (msg.getTimeout() > 0) {

            // Use the priority as a fraction to ensure that the messages with the same priority
            // Gets ordered for within that one millisecond duration
            BigDecimal timeout = new BigDecimal(now + msg.getTimeout());
            BigDecimal divideByOne =
                    BigDecimal.ONE.divide(new BigDecimal(msg.getPriority() + 1), PRECISION_MC);
            BigDecimal oneMinusDivByOne = BigDecimal.ONE.subtract(divideByOne);
            BigDecimal bd = timeout.add(oneMinusDivByOne);
            score = bd.doubleValue();

        } else {
            // double score = now + msg.getTimeout() + priority;     --> This was the old logic -
            // for the reference
            score = msg.getPriority() > 0 ? msg.getPriority() : now;
        }

        return score;
    }
}
