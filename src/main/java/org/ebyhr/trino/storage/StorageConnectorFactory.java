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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.hive.HiveHdfsModule;
import io.trino.plugin.hive.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class StorageConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "storage";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new StorageHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new StorageModule(catalogName, context.getTypeManager()),
                    new HiveHdfsModule(),
                    new HiveS3Module(),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HdfsAuthenticationModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(StorageConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
