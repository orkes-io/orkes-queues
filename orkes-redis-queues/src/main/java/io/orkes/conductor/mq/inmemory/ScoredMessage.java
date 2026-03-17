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

import java.util.Objects;

/**
 * A message entry in the sorted set, ordered by score (ascending) then messageId for
 * deterministic tie-breaking. Mirrors the behavior of a Redis ZSET member+score pair.
 */
public class ScoredMessage implements Comparable<ScoredMessage> {

    private final double score;
    private final String messageId;

    public ScoredMessage(double score, String messageId) {
        this.score = score;
        this.messageId = messageId;
    }

    public double getScore() {
        return score;
    }

    public String getMessageId() {
        return messageId;
    }

    @Override
    public int compareTo(ScoredMessage other) {
        int cmp = Double.compare(this.score, other.score);
        if (cmp != 0) {
            return cmp;
        }
        return this.messageId.compareTo(other.messageId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScoredMessage that = (ScoredMessage) o;
        return Double.compare(that.score, score) == 0
                && Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, messageId);
    }
}
