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
package org.ebyhr.trino.storage.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;
import org.ebyhr.trino.storage.StorageClient;
import org.ebyhr.trino.storage.StorageColumn;
import org.ebyhr.trino.storage.StorageTable;
import org.ebyhr.trino.storage.StorageTableHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.StorageSplit.Mode.TABLE;

public class ReadFileTableFunction
        implements Provider<ConnectorTableFunction>
{
    private final StorageClient storageClient;

    @Inject
    public ReadFileTableFunction(StorageClient storageClient)
    {
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(storageClient);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final StorageClient storageClient;

        public QueryFunction(StorageClient storageClient)
        {
            super(
                    "system",
                    "read_file",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("TYPE")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("PATH")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
            this.storageClient = requireNonNull(storageClient, "storageClient is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            String type = ((Slice) ((ScalarArgument) arguments.get("TYPE")).getValue()).toStringUtf8();
            String path = ((Slice) ((ScalarArgument) arguments.get("PATH")).getValue()).toStringUtf8();

            StorageTable table = storageClient.getTable(session, type, path);

            Descriptor returnedType = new Descriptor(table.getColumns().stream()
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            ReadFunctionHandle handle = new ReadFunctionHandle(new StorageTableHandle(TABLE, type, path), table.getColumns());

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class ReadFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final StorageTableHandle tableHandle;
        private final List<StorageColumn> columns;

        @JsonCreator
        public ReadFunctionHandle(
                @JsonProperty("tableHandle") StorageTableHandle tableHandle,
                @JsonProperty("columns") List<StorageColumn> columns)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public List<StorageColumn> getColumns()
        {
            return columns;
        }
    }
}
