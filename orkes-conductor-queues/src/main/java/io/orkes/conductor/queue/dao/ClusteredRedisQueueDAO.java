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
package io.orkes.conductor.queue.dao;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.dao.QueueDAO;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.redis.cluster.ConductorRedisClusterQueue;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

@Slf4j
public class ClusteredRedisQueueDAO extends BaseRedisQueueDAO implements QueueDAO {

    private final JedisCluster jedisCluster;

    private final MeterRegistry registry;

    public ClusteredRedisQueueDAO(
            MeterRegistry registry,
            JedisCluster jedisCluster,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties conductorProperties) {

        super(queueRedisProperties, conductorProperties);
        this.registry = registry;
        this.jedisCluster = jedisCluster;
        log.info("Queues initialized using {}", ClusteredRedisQueueDAO.class.getName());
    }

    @Override
    protected ConductorQueue getConductorQueue(String queueKey) {
        ConductorRedisClusterQueue queue = new ConductorRedisClusterQueue(queueKey, jedisCluster);
        return queue;
    }
}
