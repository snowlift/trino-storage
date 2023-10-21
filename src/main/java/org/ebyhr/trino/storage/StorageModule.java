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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpClientConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.ebyhr.trino.storage.ptf.ListTableFunction;
import org.ebyhr.trino.storage.ptf.ReadFileTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class StorageModule
        implements Module
{
    private final TypeManager typeManager;

    public StorageModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(StorageConnector.class).in(Scopes.SINGLETON);
        binder.bind(StorageMetadata.class).in(Scopes.SINGLETON);
        binder.bind(StorageClient.class).in(Scopes.SINGLETON);
        binder.bind(StorageSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(StorageRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(StoragePageSourceProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(ReadFileTableFunction.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(ListTableFunction.class).in(Scopes.SINGLETON);
        binder.bind(TrinoFileSystemFactory.class).to(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
        binder.bind(TrinoHdfsFileSystemStats.class).in(Scopes.SINGLETON);
        binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop());
        configBinder(binder).bindConfig(StorageConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(StorageTable.class));

        configBinder(binder).bindConfig(HttpClientConfig.class, ForStorage.class);
        httpClientBinder(binder).bindHttpClient("storage", ForStorage.class);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(new TypeSignature(value));
        }
    }
}
