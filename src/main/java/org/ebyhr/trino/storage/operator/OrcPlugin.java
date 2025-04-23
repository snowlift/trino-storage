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
package org.ebyhr.trino.storage.operator;

import io.trino.orc.FileOrcDataSource;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.ebyhr.trino.storage.operator.OrcTypeTranslator.fromOrcType;
import static org.joda.time.DateTimeZone.UTC;

public class OrcPlugin
        implements FilePlugin
{
    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try (ClosableFile file = getLocalFile(path, streamProvider)) {
            OrcReader reader = getReader(file.getFile());
            ColumnMetadata<OrcType> types = reader.getFooter().getTypes();
            return reader.getRootColumn().getNestedColumns().stream()
                    .map(orcColumn -> new StorageColumnHandle(
                            orcColumn.getColumnName(),
                            fromOrcType(types.get(orcColumn.getColumnId()), types)))
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorPageSource getConnectorPageSource(String path, List<String> handleColumns, Function<String, InputStream> streamProvider)
    {
        try (ClosableFile file = getLocalFile(path, streamProvider)) {
            OrcReader reader = getReader(file.getFile());
            OrcDataSource dataSource = new FileOrcDataSource(file.getFile(), new OrcReaderOptions());

            ColumnMetadata<OrcType> types = reader.getFooter().getTypes();
            List<OrcColumn> handleOrcColumns = reader.getRootColumn().getNestedColumns().stream()
                    .filter(orcColumn -> handleColumns.contains(orcColumn.getColumnName().toLowerCase()))
                    .toList();
            List<Type> readTypes = handleOrcColumns.stream()
                    .map(orcColumn -> fromOrcType(types.get(orcColumn.getColumnId()), types))
                    .collect(Collectors.toList());
            OrcRecordReader recordReader = reader.createRecordReader(
                    handleOrcColumns,
                    readTypes,
                    false,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    OrcPlugin::handleException);
            return new OrcPageSource(recordReader,
                    dataSource,
                    newSimpleAggregatedMemoryContext(),
                    new FileFormatDataSourceStats(),
                    CompressionKind.NONE);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ClosableFile getLocalFile(String path, Function<String, InputStream> streamProvider)
            throws IOException
    {
        if (path.startsWith("http://") || path.startsWith("https://") || path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
            AutoDeletingTempFile tempFile = new AutoDeletingTempFile();
            Files.copy(streamProvider.apply(path), tempFile.getFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        }
        if (path.startsWith("file:")) {
            return () -> new File(path.substring(5));
        }
        throw new IllegalArgumentException(format("Unsupported schema %s", path.split(":", 2)[0]));
    }

    private OrcReader getReader(File file)
    {
        OrcDataSource dataSource;
        try {
            dataSource = new FileOrcDataSource(file, new OrcReaderOptions());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Optional<OrcReader> reader;
        try {
            reader = createOrcReader(dataSource, new OrcReaderOptions());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (reader.isEmpty()) {
            throw new RuntimeException("Failed to create an ORC reader");
        }
        return reader.get();
    }

    private static TrinoException handleException(Exception e)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read temporary data", e);
    }

    public interface ClosableFile
            extends AutoCloseable
    {
        File getFile();

        @Override
        default void close()
                throws IOException
        {
        }
    }

    public static class AutoDeletingTempFile
            implements ClosableFile
    {
        private final File file;

        public AutoDeletingTempFile()
                throws IOException
        {
            file = File.createTempFile("trino-storage-", ".orc");
        }

        @Override
        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
                throws IOException
        {
            if (!file.delete()) {
                throw new IOException(format("Failed to delete temp file %s", file));
            }
        }
    }
}
