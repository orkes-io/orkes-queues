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

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import com.netflix.conductor.core.config.ConductorProperties;

import io.orkes.conductor.queue.config.QueueRedisProperties;

public class InMemoryQueueDAOTest extends BaseQueueDAOTest {

    @TempDir static Path tempDir;

    @BeforeAll
    public static void setUp() throws IOException {
        ConductorProperties conductorProperties = new ConductorProperties();
        QueueRedisProperties queueRedisProperties = new QueueRedisProperties(conductorProperties);
        redisQueue = new InMemoryQueueDAO(queueRedisProperties, conductorProperties, tempDir);
    }
}
