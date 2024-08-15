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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.dao.QueueDAO;

import io.orkes.conductor.mq.redis.QueueMonitorProperties;
import io.orkes.conductor.queue.dao.ClusteredRedisQueueDAO;
import io.orkes.conductor.queue.dao.RedisQueueDAO;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;

@EnableAutoConfiguration
@AutoConfiguration
@Slf4j
@Import({io.orkes.conductor.queue.config.QueueRedisProperties.class})
public class RedisQueueConfiguration {

    protected static final int DEFAULT_MAX_ATTEMPTS = 5;

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_standalone")
    public QueueDAO getQueueDAOStandalone(
            JedisPool jedisPool,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties,
            QueueMonitorProperties queueMonitorProperties) {
        log.info("getQueueDAOStandalone init");
        return new RedisQueueDAO(
                registry, jedisPool, queueRedisProperties, properties, queueMonitorProperties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_sentinel")
    public QueueDAO getQueueDAOSentinel(
            JedisSentinelPool jedisSentinelPool,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties,
            QueueMonitorProperties queueMonitorProperties) {
        return new RedisQueueDAO(
                registry,
                jedisSentinelPool,
                queueRedisProperties,
                properties,
                queueMonitorProperties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_cluster")
    public QueueDAO getQueueDAOCluster(
            JedisCluster jedisCluster,
            MeterRegistry registry,
            QueueRedisProperties queueRedisProperties,
            ConductorProperties properties,
            QueueMonitorProperties queueMonitorProperties) {
        return new ClusteredRedisQueueDAO(
                registry, jedisCluster, queueRedisProperties, properties, queueMonitorProperties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_standalone")
    protected JedisPool getJedisPoolStandalone(QueueRedisProperties redisProperties) {
        ConfigurationHostSupplier hostSupplier = new ConfigurationHostSupplier(redisProperties);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(2);
        config.setMaxTotal(redisProperties.getMaxConnectionsPerHost());
        log.info(
                "Starting conductor server using redis_standalone - use SSL? {}",
                redisProperties.isSsl());
        Host host = hostSupplier.getHosts().get(0);

        if (redisProperties.getUsername() != null && host.getPassword() != null) {
            log.info("Connecting to Redis Standalone with ACL user AUTH");
            return new JedisPool(
                    config,
                    host.getHostName(),
                    host.getPort(),
                    Protocol.DEFAULT_TIMEOUT,
                    redisProperties.getUsername(),
                    host.getPassword(),
                    redisProperties.getDatabase(),
                    redisProperties.isSsl());
        } else if (host.getPassword() != null) {
            log.info("Connecting to Redis Standalone with AUTH");
            return new JedisPool(
                    config,
                    host.getHostName(),
                    host.getPort(),
                    Protocol.DEFAULT_TIMEOUT,
                    host.getPassword(),
                    redisProperties.getDatabase(),
                    redisProperties.isSsl());
        } else {
            return new JedisPool(
                    config,
                    host.getHostName(),
                    host.getPort(),
                    Protocol.DEFAULT_TIMEOUT,
                    null,
                    redisProperties.getDatabase(),
                    redisProperties.isSsl());
        }
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_sentinel")
    public JedisSentinelPool getJedisPoolSentinel(QueueRedisProperties properties) {
        ConfigurationHostSupplier hostSupplier = new ConfigurationHostSupplier(properties);
        GenericObjectPoolConfig<?> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
        genericObjectPoolConfig.setMinIdle(properties.getMinIdleConnections());
        genericObjectPoolConfig.setMaxIdle(properties.getMaxIdleConnections());
        genericObjectPoolConfig.setMaxTotal(properties.getMaxConnectionsPerHost());
        genericObjectPoolConfig.setTestWhileIdle(properties.isTestWhileIdle());
        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(
                properties.getMinEvictableIdleTimeMillis());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(
                properties.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setNumTestsPerEvictionRun(properties.getNumTestsPerEvictionRun());
        log.info(
                "Starting conductor server using redis_sentinel and cluster "
                        + properties.getClusterName());
        Set<String> sentinels = new HashSet<>();
        for (Host host : hostSupplier.getHosts()) {
            sentinels.add(host.getHostName() + ":" + host.getPort());
        }
        // We use the password of the first sentinel host as password and sentinelPassword
        String password = getPassword(hostSupplier.getHosts());
        if (properties.getUsername() != null && password != null) {
            return new JedisSentinelPool(
                    properties.getClusterName(),
                    sentinels,
                    genericObjectPoolConfig,
                    Protocol.DEFAULT_TIMEOUT,
                    Protocol.DEFAULT_TIMEOUT,
                    properties.getUsername(),
                    password,
                    properties.getDatabase(),
                    null,
                    Protocol.DEFAULT_TIMEOUT,
                    Protocol.DEFAULT_TIMEOUT,
                    properties.getUsername(),
                    password,
                    null);

        } else if (password != null) {
            return new JedisSentinelPool(
                    properties.getClusterName(),
                    sentinels,
                    genericObjectPoolConfig,
                    Protocol.DEFAULT_TIMEOUT,
                    Protocol.DEFAULT_TIMEOUT,
                    password,
                    properties.getDatabase(),
                    null,
                    Protocol.DEFAULT_TIMEOUT,
                    Protocol.DEFAULT_TIMEOUT,
                    password,
                    null);
        } else {
            return new JedisSentinelPool(
                    properties.getClusterName(),
                    sentinels,
                    genericObjectPoolConfig,
                    Protocol.DEFAULT_TIMEOUT,
                    null,
                    properties.getDatabase());
        }
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "conductor.queue.type", havingValue = "redis_cluster")
    public JedisCluster createJedisCommands(QueueRedisProperties properties) {
        ConfigurationHostSupplier hostSupplier = new ConfigurationHostSupplier(properties);

        GenericObjectPoolConfig<?> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
        genericObjectPoolConfig.setMaxTotal(properties.getMaxConnectionsPerHost());

        Set<HostAndPort> hosts =
                hostSupplier.getHosts().stream()
                        .map(h -> new HostAndPort(h.getHostName(), h.getPort()))
                        .collect(Collectors.toSet());
        String password = getPassword(hostSupplier.getHosts());

        if (password != null) {
            log.info("Connecting to Redis Cluster with AUTH");
        }

        return new JedisCluster(
                hosts,
                Protocol.DEFAULT_TIMEOUT,
                Protocol.DEFAULT_TIMEOUT,
                DEFAULT_MAX_ATTEMPTS,
                password,
                null,
                genericObjectPoolConfig,
                properties.isSsl());
    }

    private String getPassword(List<Host> hosts) {
        return hosts.isEmpty() ? null : hosts.get(0).getPassword();
    }
}
