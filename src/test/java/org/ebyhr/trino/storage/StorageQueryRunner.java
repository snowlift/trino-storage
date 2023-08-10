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

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class StorageQueryRunner
{
    private StorageQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createStorageQueryRunner(
            Optional<TestingStorageServer> storageServer,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new StoragePlugin());
            queryRunner.createCatalog("storage", "storage", connectorProperties);

            storageServer.ifPresent(server -> {
                server.getHadoopServer().copyFromLocal("example-data/lineitem-1.csv", "/tmp/lineitem-1.csv", "/tmp/lineitem-1");
                server.getHadoopServer().copyFromLocal("example-data/numbers.tsv", "/tmp/numbers.tsv", "/tmp/numbers.tsv");
            });

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive").build())
                .setCatalog("storage")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static final class StorageHadoopQueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();

            TestingStorageServer storageServer = new TestingStorageServer();
            DistributedQueryRunner queryRunner = createStorageQueryRunner(
                    Optional.of(storageServer),
                    Map.of("http-server.http.port", "8080"),
                    Map.of(
                            "hive.s3.path-style-access", "true",
                            "hive.s3.endpoint", storageServer.getMinioServer().getEndpoint(),
                            "hive.s3.aws-access-key", TestingMinioServer.ACCESS_KEY,
                            "hive.s3.aws-secret-key", TestingMinioServer.SECRET_KEY));

            Logger log = Logger.get(StorageQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class StorageLocalQueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();

            DistributedQueryRunner queryRunner = createStorageQueryRunner(Optional.empty(), Map.of("http-server.http.port", "8080"), Map.of());

            Logger log = Logger.get(StorageQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
