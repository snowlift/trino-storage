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
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.ebyhr.trino.storage.StorageColumn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.ebyhr.trino.storage.operator.ParquetTypeTranslator.fromParquetType;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetPlugin
        implements FilePlugin
{
    @Override
    public List<StorageColumn> getFields(String path, Function<String, InputStream> streamProvider)
    {
        path = getLocalPath(path, streamProvider);
        MessageType schema = getSchema(new File(path));
        return schema.getFields().stream()
                .map(field -> new StorageColumn(
                        field.getName(),
                        fromParquetType(field)))
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider)
    {
        path = getLocalPath(path, streamProvider);
        ParquetReader reader = getReader(new File(path));

        ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
        ImmutableList.Builder<Boolean> rowIndexChannels = ImmutableList.builder();

        // TODO assuming all columns are being read, populate the above lists
        MessageType schema = getSchema(new File(path));
        MessageColumnIO messageColumnIO = getColumnIO(schema, new MessageType(schema.getName(), schema.getFields()));
        schema.getFields().forEach(field -> {
            Type trinoType = fromParquetType(field);
            trinoTypes.add(trinoType);
            rowIndexChannels.add(false);
            internalFields.add(constructField(trinoType, messageColumnIO.getChild(field.getName())));
        });

        ParquetPageSource pageSource = ParquetPageSource.builder().build(reader)(reader, trinoTypes.build(), rowIndexChannels.build(), internalFields.build());
        List<Page> result = new LinkedList<>();
        Page page;
        while ((page = pageSource.getNextPage()) != null) {
            result.add(page.getLoadedPage());
        }
        return result;
    }

    private String getLocalPath(String path, Function<String, InputStream> streamProvider)
    {
        if (path.startsWith("http://") || path.startsWith("https://") || path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
            try (AutoDeletingTempFile tempFile = new AutoDeletingTempFile()) {
                Files.copy(streamProvider.apply(path), tempFile.getFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
                return tempFile.getFile().getPath();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (path.startsWith("file:")) {
            return path.substring(5);
        }
        return path;
    }

    private MessageType getSchema(File file)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        MessageType fileSchema = null;
        ParquetDataSource dataSource = null;
        try {
            dataSource = new FileParquetDataSource(file, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();
        }
        catch (Exception e) {
            handleException(file, dataSource, e);
        }
        return fileSchema;
    }

    private ParquetReader getReader(File file)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        MessageType fileSchema;
        MessageType requestedSchema;
        MessageColumnIO messageColumn;
        ParquetReader parquetReader = null;
        ParquetDataSource dataSource = null;
        try {
            dataSource = new FileParquetDataSource(file, options);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            fileSchema = fileMetaData.getSchema();

            requestedSchema = new MessageType(fileSchema.getName(), fileSchema.getFields());
            messageColumn = getColumnIO(fileSchema, requestedSchema);

            long nextStart = 0;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                blocks.add(block);
                blockStarts.add(nextStart);
                nextStart += block.getRowCount();
            }
            parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumn,
                    blocks.build(),
                    blockStarts.build(),
                    dataSource,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    options);
        }
        catch (Exception e) {
            handleException(file, dataSource, e);
        }
        return parquetReader;
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
            throw new TrinoException(HIVE_BAD_DATA, e);
        }
        if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                e instanceof FileNotFoundException) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
        String message = format("Error opening Parquet file %s: %s", file, e.getMessage());
        throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
    }

    private static TrinoException handleException(Exception e)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read temporary data", e);
    }

    public static class AutoDeletingTempFile
            implements AutoCloseable
    {
        private final File file;

        public AutoDeletingTempFile()
                throws IOException
        {
            file = File.createTempFile("trino-storage-", ".orc");
        }

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

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                String name = rowField.getName().orElseThrow().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(rowField.getType(), lookupColumnByName(groupColumnIO, name));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(arrayType.getElementType(), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        ColumnDescriptor column = new ColumnDescriptor(primitiveColumnIO.getFieldPath(), columnIO.getType().asPrimitiveType(), repetitionLevel, definitionLevel);
        return Optional.of(new PrimitiveField(type, required, column, primitiveColumnIO.getId()));
    }
}
