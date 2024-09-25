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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;
import org.ebyhr.trino.storage.operator.FilePlugin;
import org.ebyhr.trino.storage.operator.PluginFactory;

import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class StorageClient2
{
    private static final Logger log = Logger.get(StorageClient.class);

    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public StorageClient(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    public List<String> getSchemaNames()
    {
        return Stream.of(FileType.values())
                .map(FileType::toString)
                .collect(Collectors.toList());
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return new HashSet<>();
    }

    public StorageTable getTable(ConnectorSession session, String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        FilePlugin plugin = PluginFactory.create(schema);
        try (InputStream inputStream = getInputStream(session, tableName)) {
            List<StorageColumn> columns = plugin.getFields(inputStream);
            return new StorageTable(tableName, columns);
        }
        catch (Exception e) {
            log.error(e, "Failed to get table: %s.%s", schema, tableName);
            return null;
        }
    }

    public InputStream getInputStream(ConnectorSession session, String path)
            throws IOException
    {
        if (path.startsWith("http://") || path.startsWith("https://")) {
            URL url = new URL(path);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            return connection.getInputStream();
        }
        if (path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
            Path hdfsPath = new Path(path);
            return hdfsEnvironment.getFileSystem(new HdfsContext(session), hdfsPath).open(hdfsPath);
        }

        return URI.create(path).toURL().openStream();
    }
}
