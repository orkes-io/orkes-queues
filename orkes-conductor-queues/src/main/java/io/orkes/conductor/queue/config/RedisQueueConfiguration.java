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
package io.orkes.conductor.queue.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.dao.QueueDAO;

import io.orkes.conductor.queue.dao.ClusteredRedisQueueDAO;
import io.orkes.conductor.queue.dao.QueueRedisProperties;
import io.orkes.conductor.queue.dao.RedisQueueDAO;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

@Configuration
@Slf4j
public class RedisQueueConfiguration {

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_standalone")
    public QueueDAO getQueueDAOStandalone(
            JedisPool jedisPool,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties) {
        return new RedisQueueDAO(registry, jedisPool, queueRedisProperties, properties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_sentinel")
    public QueueDAO getQueueDAOSentinel(
            JedisSentinelPool jedisSentinelPool,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties) {
        return new RedisQueueDAO(registry, jedisSentinelPool, queueRedisProperties, properties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_cluster")
    public QueueDAO getQueueDAOCluster(
            JedisCluster jedisCluster,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties) {
        return new ClusteredRedisQueueDAO(registry, jedisCluster, queueRedisProperties, properties);
    }
}
