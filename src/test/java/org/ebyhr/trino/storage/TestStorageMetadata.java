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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.ebyhr.trino.storage.StorageSplit.Mode.TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestStorageMetadata
{
    private StorageTableHandle numbersTableHandle;
    private StorageMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        URL numbersUrl = Resources.getResource(TestStorageClient.class, "/example-data/numbers-1.csv");
        numbersTableHandle = new StorageTableHandle(TABLE, "csv", numbersUrl.toString());

        URL metadataUrl = Resources.getResource(TestStorageClient.class, "/example-data/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");

        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(hdfsEnvironment);

        StorageClient client = new StorageClient(fileSystemFactory);
        metadata = new StorageMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsOnly("csv", "tsv", "txt", "raw", "excel", "orc", "json");
    }

    @Test
    public void testGetTableHandle()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, numbersTableHandle), Map.of(
                "one", new StorageColumnHandle("one", createUnboundedVarcharType()),
                "1", new StorageColumnHandle("1", createUnboundedVarcharType())));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new StorageTableHandle(TABLE, "unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (SchemaNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new StorageTableHandle(TABLE, "csv", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, numbersTableHandle);
        assertEquals(tableMetadata.getTable().getSchemaName(), "csv");
        assertEquals(tableMetadata.getColumns(), List.of(
                new ColumnMetadata("one", createUnboundedVarcharType()),
                new ColumnMetadata("1", createUnboundedVarcharType())));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(TABLE, "unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(TABLE, "example", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(TABLE, "unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.empty())), Set.of());

        // specific schema
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.of("tsv"))), Set.of());
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.of("csv"))), Set.of());

        // unknown schema
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), Set.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, numbersTableHandle, new StorageColumnHandle("text", createUnboundedVarcharType())),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // StorageTableHandle and StorageColumnHandle passed in.  This is on because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testCreateTable()
    {
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("example", "foo"),
                        List.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false);
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, numbersTableHandle);
    }
}
