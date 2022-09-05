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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.core.config.ConductorProperties;

import lombok.Data;

@Configuration
@ConfigurationProperties("conductor.redis")
@Data
public class QueueRedisProperties {

    private final ConductorProperties conductorProperties;

    @Autowired
    public QueueRedisProperties(ConductorProperties conductorProperties) {
        this.conductorProperties = conductorProperties;
    }

    /**
     * Local rack / availability zone. For AWS deployments, the value is something like us-east-1a,
     * etc.
     */
    private String availabilityZone = "us-east-1c";

    /** Redis Cluster details. Format is host:port:rack separated by semicolon */
    private String hosts = null;

    /** The prefix used to prepend workflow data in redis */
    private String workflowNamespacePrefix = null;

    /** The prefix used to prepend keys for queues in redis */
    private String queueNamespacePrefix = null;

    /**
     * The domain name to be used in the key prefix for logical separation of workflow data and
     * queues in a shared redis setup
     */
    private String keyspaceDomain = null;

    /**
     * The maximum number of connections that can be managed by the connection pool on a given
     * instance
     */
    private int maxConnectionsPerHost = 10;

    public String getQueuePrefix() {
        String prefix = getQueueNamespacePrefix() + "." + conductorProperties.getStack();
        if (getKeyspaceDomain() != null) {
            prefix = prefix + "." + getKeyspaceDomain();
        }
        return prefix;
    }
}
