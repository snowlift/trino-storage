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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.spi.StandardErrorCode.CORRUPT_PAGE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static org.ebyhr.trino.storage.operator.ParquetTypeTranslator.fromParquetType;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetPlugin
        implements FilePlugin
{
    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try (ClosableFile file = getLocalFile(path, streamProvider)) {
            MessageType schema = getSchema(file.getFile());
            return schema.getFields().stream()
                    .map(field -> new StorageColumnHandle(
                            field.getName(),
                            fromParquetType(field)))
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
            ParquetReader reader = getReader(file.getFile(), handleColumns);
            return new ParquetPageSource(reader);
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

    private MessageType getSchema(File file)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        MessageType fileSchema = null;
        ParquetDataSource dataSource = null;
        try {
            dataSource = new FileParquetDataSource(file, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();
        }
        catch (Exception e) {
            handleException(file, dataSource, e);
        }
        return fileSchema;
    }

    private ParquetReader getReader(File file, List<String> handleColumns)
            throws IOException
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        ParquetDataSource dataSource;
        dataSource = new FileParquetDataSource(file, options);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
        MessageColumnIO messageColumnIO = getColumnIO(fileMetaData.getSchema(), fileMetaData.getSchema());
        ImmutableList.Builder<Column> columnFields = ImmutableList.builder();
        for (org.apache.parquet.schema.Type type : fileMetaData.getSchema().getFields()) {
            if (handleColumns.contains(type.getName().toLowerCase())) {
                columnFields.add(new Column(
                        messageColumnIO.getName(),
                        constructField(
                            fromParquetType(type),
                            lookupColumnByName(messageColumnIO, type.getName()))
                            .orElseThrow()));
            }
        }
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileMetaData.getSchema(), fileMetaData.getSchema());
        long nextStart = 0;
        ImmutableList.Builder<RowGroupInfo> rowGroupInfoBuilder = ImmutableList.builder();
        for (BlockMetadata block : parquetMetadata.getBlocks()) {
            rowGroupInfoBuilder.add(
                    new RowGroupInfo(
                            createPrunedColumnsMetadata(
                                    block,
                                    dataSource.getId(),
                                    descriptorsByPath),
                            nextStart,
                            Optional.empty()));
            nextStart += block.rowCount();
        }
        return new ParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                false,
                rowGroupInfoBuilder.build(),
                dataSource,
                UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions(),
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.empty(),
                Optional.empty());
    }

    private void handleException(File file, ParquetDataSource dataSource, Exception e)
    {
        try {
            if (dataSource != null) {
                dataSource.close();
            }
        }
        catch (IOException ignored) {
        }
        if (e instanceof TrinoException) {
            throw (TrinoException) e;
        }
        if (e instanceof ParquetCorruptionException) {
            throw new TrinoException(CORRUPT_PAGE, e);
        }
        if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                e instanceof FileNotFoundException) {
            throw new TrinoException(NOT_FOUND, e);
        }
        String message = format("Error opening Parquet file %s: %s", file, e.getMessage());
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message);
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
            file = File.createTempFile("trino-storage-", ".parquet");
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

    /**
     * this class is copied from io.trino.parquet.reader.FileParquetDataSource because it is in test-jar
     */
    public static class FileParquetDataSource
            extends AbstractParquetDataSource
    {
        private final RandomAccessFile input;

        public FileParquetDataSource(File path, ParquetReaderOptions options)
                throws FileNotFoundException
        {
            super(new ParquetDataSourceId(path.getPath()), path.length(), options);
            this.input = new RandomAccessFile(path, "r");
        }

        @Override
        public void close()
                throws IOException
        {
            super.close();
            input.close();
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }
    }
}
