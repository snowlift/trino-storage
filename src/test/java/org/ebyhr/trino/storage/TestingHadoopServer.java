/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import com.google.common.collect.ImmutableList;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;

import static java.lang.String.format;

public class TestingHadoopServer
        implements Closeable
{
    private static final String HOST_NAME = "hadoop-master";

    private final GenericContainer<?> dockerContainer;

    public TestingHadoopServer()
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse("ghcr.io/trinodb/testing/hdp3.1-hive:41"))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST_NAME))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy());
        dockerContainer.setPortBindings(ImmutableList.of("1180:1180", "9000:9000"));
        dockerContainer.start();
    }

    public String getSocksProxy()
    {
        return format("%s:1180", HOST_NAME);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
