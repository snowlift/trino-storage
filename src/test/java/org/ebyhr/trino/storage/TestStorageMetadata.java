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
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestStorageMetadata
{
    private static final String CONNECTOR_ID = "TEST";
    private StorageTableHandle numbersTableHandle;
    private StorageMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        URL numbersUrl = Resources.getResource(TestStorageClient.class, "/example-data/numbers-1.csv");
        numbersTableHandle = new StorageTableHandle(CONNECTOR_ID, "csv", numbersUrl.toString());

        URL metadataUrl = Resources.getResource(TestStorageClient.class, "/example-data/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");

        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        StorageClient client = new StorageClient(hdfsEnvironment);
        metadata = new StorageMetadata(new StorageConnectorId(CONNECTOR_ID), client);
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
                "one", new StorageColumnHandle(CONNECTOR_ID, "one", createUnboundedVarcharType(), 0),
                "1", new StorageColumnHandle(CONNECTOR_ID, "1", createUnboundedVarcharType(), 1)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new StorageTableHandle(CONNECTOR_ID, "unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (SchemaNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new StorageTableHandle(CONNECTOR_ID, "csv", "unknown"));
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
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(CONNECTOR_ID, "unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(CONNECTOR_ID, "example", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new StorageTableHandle(CONNECTOR_ID, "unknown", "numbers")));
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
        assertEquals(metadata.getColumnMetadata(SESSION, numbersTableHandle, new StorageColumnHandle(CONNECTOR_ID, "text", createUnboundedVarcharType(), 0)),
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
