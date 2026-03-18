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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface ConductorQueue {

    static final BigDecimal HUNDRED = new BigDecimal(100);

    static final BigDecimal MILLION = new BigDecimal(1000_000);

    static final MathContext PRECISION_MC = new MathContext(20);

    String getName();

    List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit);

    boolean ack(String messageId);

    void push(List<QueueMessage> messages);

    boolean setUnacktimeout(String messageId, long unackTimeout);

    boolean exists(String messageId);

    void remove(String messageId);

    QueueMessage get(String messageId);

    void flush();

    long size();

    int getQueueUnackTime();

    void setQueueUnackTime(int queueUnackTime);

    String getShardName();

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
