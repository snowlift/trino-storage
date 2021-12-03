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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;

import static java.lang.String.format;

public class TestingMinioServer
        implements Closeable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    private static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2021-07-15T22-27-34Z";
    private static final String HOSTNAME = "minio";

    private static final int API_PORT = 4566;
    private static final int CONSOLE_PORT = 4567;

    private final GenericContainer<?> container;
    private final AmazonS3 s3Client;

    public TestingMinioServer(Network network)
    {
        container = new GenericContainer<>(DockerImageName.parse(DEFAULT_IMAGE))
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOSTNAME))
                .withCommand("server",
                        "--address", "0.0.0.0:" + API_PORT,
                        "--console-address", "0.0.0.0:" + CONSOLE_PORT,
                        "/data")
                .withEnv("MINIO_ACCESS_KEY", ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", SECRET_KEY)
                .withExposedPorts(API_PORT, CONSOLE_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy())
                .withNetwork(network);
        container.start();

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + container.getMappedPort(API_PORT), "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    public String getEndpoint()
    {
        return format("http://%s:%s", container.getHost(), container.getMappedPort(API_PORT));
    }

    public void createBucket(String name)
    {
        s3Client.createBucket(name);
    }

    public void createFile(String bucketName, String fileName)
    {
        s3Client.putObject(bucketName, fileName, "hello");
    }

    @Override
    public void close()
    {
        container.close();
    }
}
