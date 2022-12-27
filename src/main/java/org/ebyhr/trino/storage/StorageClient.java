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
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.ebyhr.trino.storage.operator.FilePlugin;
import org.ebyhr.trino.storage.operator.PluginFactory;

import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.ptf.ListTableFunction.LIST_SCHEMA_NAME;

public class StorageClient
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

        if (schema.equals(LIST_SCHEMA_NAME)) {
            return new StorageTable(StorageSplit.Mode.LIST, tableName, List.of(new StorageColumn("path", VarcharType.VARCHAR)));
        }

        FilePlugin plugin = PluginFactory.create(schema);
        try {
            List<StorageColumn> columns = plugin.getFields(tableName, path -> getInputStream(session, path));
            return new StorageTable(StorageSplit.Mode.TABLE, tableName, columns);
        }
        catch (Exception e) {
            log.error(e, "Failed to get table: %s.%s", schema, tableName);
            return null;
        }
    }

    public InputStream getInputStream(ConnectorSession session, String path)
    {
        try {
            if (path.startsWith("http://") || path.startsWith("https://")) {
                URL url = new URL(path);
                HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
                return connection.getInputStream();
            }
            if (path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
                Path hdfsPath = new Path(path);
                return hdfsEnvironment.getFileSystem(new HdfsContext(session), hdfsPath).open(hdfsPath);
            }
            if (!path.startsWith("file:")) {
                path = "file:" + path;
            }

            return URI.create(path).toURL().openStream();
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to open stream for %s", path), e);
        }
    }

    public FileStatus[] list(ConnectorSession session, String path)
    {
        Path hadoopPath = new Path(path);
        try {
            return hdfsEnvironment.getFileSystem(new HdfsContext(session), hadoopPath).listStatus(hadoopPath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
