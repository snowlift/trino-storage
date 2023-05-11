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

import com.google.common.collect.Iterables;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.ebyhr.trino.storage.operator.FilePlugin;
import org.ebyhr.trino.storage.operator.PluginFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class StorageRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final StorageClient storageClient;

    @Inject
    public StorageRecordSetProvider(StorageClient storageClient)
    {
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        StorageSplit storageSplit = (StorageSplit) split;

        String schemaName = storageSplit.getSchemaName();
        String tableName = storageSplit.getTableName();
        StorageTable storageTable = storageClient.getTable(session, schemaName, tableName);

        FilePlugin plugin = PluginFactory.create(schemaName);
        Stream<List<?>> stream = plugin.getRecordsIterator(tableName, path -> storageClient.getInputStream(session, path));
        Iterable<List<?>> rows = stream::iterator;

        List<StorageColumnHandle> handles = columns
                .stream()
                .map(c -> (StorageColumnHandle) c)
                .collect(toList());
        List<Integer> columnIndexes = handles
                .stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : storageTable.getColumnsMetadata()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(rows, row -> columnIndexes
                .stream()
                .map(row::get)
                .collect(toList()));

        List<Type> mappedTypes = handles
                .stream()
                .map(StorageColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }
}
