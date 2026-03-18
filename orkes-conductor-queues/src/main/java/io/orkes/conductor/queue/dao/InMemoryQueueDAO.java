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
package io.orkes.conductor.queue.dao;

import java.nio.file.Path;
import java.util.Map;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.dao.QueueDAO;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.inmemory.ConductorInMemoryQueue;
import io.orkes.conductor.mq.inmemory.QueueStatePersistence;
import io.orkes.conductor.mq.inmemory.WriteAheadLog;
import io.orkes.conductor.queue.config.QueueRedisProperties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryQueueDAO extends BaseRedisQueueDAO implements QueueDAO {

    private final WriteAheadLog wal;
    private final Map<String, QueueStatePersistence.QueueState> preloadedStates;

    public InMemoryQueueDAO(
            QueueRedisProperties queueRedisProperties,
            ConductorProperties conductorProperties,
            Path dataDir) {

        super(queueRedisProperties, conductorProperties);
        this.wal = new WriteAheadLog(dataDir);
        this.preloadedStates = wal.loadAll();
        log.info(
                "Queues initialized using {} with {} pre-loaded queues from {}",
                InMemoryQueueDAO.class.getName(),
                preloadedStates.size(),
                dataDir);
    }

    @Override
    protected ConductorQueue getConductorQueue(String queueKey) {
        QueueStatePersistence.QueueState state = preloadedStates.remove(queueKey);
        return new ConductorInMemoryQueue(queueKey, null, wal, state);
    }
}
