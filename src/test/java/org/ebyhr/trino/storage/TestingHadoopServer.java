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

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingHadoopServer
        implements Closeable
{
    private static final String HOSTNAME = "hadoop-master";

    private final GenericContainer<?> dockerContainer;
    private final String hostname;

    public TestingHadoopServer()
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse("ghcr.io/trinodb/testing/hdp3.1-hive:41"))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOSTNAME))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy());
        dockerContainer.setPortBindings(ImmutableList.of("1180:1180", "9000:9000"));
        dockerContainer.start();
        hostname = getHostName();
    }

    public void copyFromLocal(String resourceName, String containerPath, String hdfsPath)
            throws InterruptedException, IOException
    {
        dockerContainer.copyFileToContainer(MountableFile.forClasspathResource(resourceName), containerPath);
        dockerContainer.execInContainer("hdfs", "dfs", "-copyFromLocal", containerPath, hdfsPath);
    }

    public String getSocksProxy()
    {
        return format("%s:1180", hostname);
    }

    public String toHdfsPath(String path)
    {
        return format("hdfs://%s:9000%s", hostname, path);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }

    private String getHostName()
    {
        String osName = StandardSystemProperty.OS_NAME.value();
        requireNonNull(osName, "osName is null");
        switch (osName) {
            case "Mac OS X":
                return HOSTNAME;
            case "Linux":
                try {
                    return InetAddress.getLocalHost().getHostAddress();
                }
                catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalStateException(format("Trino requires Linux or Mac OS X (found %s)", osName));
        }
    }
}
