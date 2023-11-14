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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationHostSupplier {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationHostSupplier.class);

    private final QueueRedisProperties properties;

    public ConfigurationHostSupplier(QueueRedisProperties properties) {
        this.properties = properties;
    }

    public List<Host> getHosts() {
        return parseHostsFromConfig();
    }

    private List<Host> parseHostsFromConfig() {
        System.out.println("\n\n\n\n\n\n\n\n parseHostsFromConfig" + properties.toString());
        String hosts = properties.getHosts();
        if (hosts == null) {
            // FIXME This type of validation probably doesn't belong here.
            String message =
                    "Orkes 7 - Missing dynomite/redis hosts. Ensure 'workflow.dynomite.cluster.hosts' has been set in the supplied configuration.";
            log.error(message);
            throw new RuntimeException(message);
        }
        return parseHostsFrom(hosts);
    }

    private List<Host> parseHostsFrom(String hostConfig) {
        List<String> hostConfigs = Arrays.asList(hostConfig.split(";"));

        return hostConfigs.stream()
                .map(
                        hc -> {
                            String[] hostConfigValues = hc.split(":");
                            String host = hostConfigValues[0];
                            int port = Integer.parseInt(hostConfigValues[1]);
                            String rack = hostConfigValues[2];

                            if (hostConfigValues.length >= 4) {
                                String password = hostConfigValues[3];
                                return new HostBuilder()
                                        .setHostname(host)
                                        .setPort(port)
                                        .setRack(rack)
                                        .setStatus(Host.Status.Up)
                                        .setPassword(password)
                                        .createHost();
                            }
                            return new HostBuilder()
                                    .setHostname(host)
                                    .setPort(port)
                                    .setRack(rack)
                                    .setStatus(Host.Status.Up)
                                    .createHost();
                        })
                .collect(Collectors.toList());
    }
}
