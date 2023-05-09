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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
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
import io.trino.spi.type.Type;
import org.ebyhr.trino.storage.StorageColumnHandle;
import org.ebyhr.trino.storage.StorageTableHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.StorageSplit.Mode.LIST;

public class ListTableFunction
        implements Provider<ConnectorTableFunction>
{
    public static final String LIST_SCHEMA_NAME = "$trino-storage/list";
    public static final Map<String, Type> COLUMN_TYPES = ImmutableMap.of(
            "file_modified_time", TIMESTAMP_TZ_MILLIS,
            "size", BIGINT,
            "name", VARCHAR);

    public static final List<ColumnMetadata> COLUMNS_METADATA = COLUMN_TYPES.entrySet().stream()
            .map(column -> new ColumnMetadata(column.getKey(), column.getValue()))
            .collect(toImmutableList());
    public static final List<ColumnHandle> COLUMN_HANDLES = COLUMN_TYPES.entrySet().stream()
            .map(column -> new StorageColumnHandle(column.getKey(), column.getValue()))
            .collect(toImmutableList());

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction();
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        public QueryFunction()
        {
            super(
                    "system",
                    "list",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("PATH")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            String path = ((Slice) ((ScalarArgument) arguments.get("PATH")).getValue()).toStringUtf8();

            Descriptor returnedType = new Descriptor(COLUMN_TYPES.entrySet().stream()
                    .map(column -> new Descriptor.Field(column.getKey(), Optional.of(column.getValue())))
                    .collect(toImmutableList()));

            QueryFunctionHandle handle = new QueryFunctionHandle(new StorageTableHandle(LIST, LIST_SCHEMA_NAME, path));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final StorageTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") StorageTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
