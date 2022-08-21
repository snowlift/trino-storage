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
import org.apache.hadoop.net.NetUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.lang.String.format;

public class TestingHadoopServer
        implements Closeable
{
    private static final String HOSTNAME = "hadoop-master";

    private final GenericContainer<?> dockerContainer;
    private final String hostname;

    public TestingHadoopServer(Network network)
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse("ghcr.io/trinodb/testing/hdp2.6-hive:65"))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOSTNAME))
                .withCopyFileToContainer(MountableFile.forClasspathResource("minio/hive-core-site.xml"), "/etc/hadoop/conf/core-site.xml")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy())
                .withNetwork(network);
        dockerContainer.setPortBindings(ImmutableList.of("1180:1180", "9000:9000"));
        dockerContainer.start();
        hostname = getHostName();

        // Even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
        // (e.g: in: NameNodeProxies.createProxy)
        // This adds a static resolution for hadoop-master to docker container internal ip
        //noinspection deprecation
        NetUtils.addStaticResolution(HOSTNAME, dockerContainer.getContainerInfo().getNetworkSettings().getIpAddress());
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

    private String getHostName()
    {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
