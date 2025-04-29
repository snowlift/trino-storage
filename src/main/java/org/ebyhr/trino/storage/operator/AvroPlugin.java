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

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.ebyhr.trino.storage.operator.AvroTypeTranslator.fromAvroType;

public class AvroPlugin
        implements FilePlugin
{
    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try (InputStream input = streamProvider.apply(path);
                DataFileStream<Object> dataFileStream = new DataFileStream<>(input, new GenericDatumReader<>())) {
            Schema schema = dataFileStream.getSchema();
            return schema.getFields().stream()
                    .map(field -> new StorageColumnHandle(
                            field.name(),
                            fromAvroType(field.schema())))
                    .toList();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to read Avro file: %s", path), e);
        }
    }

    @Override
    public Iterable<Page> getPagesIterator(String path, List<String> handleColumns, Function<String, InputStream> streamProvider)
    {
        try (InputStream input = streamProvider.apply(path);
                DataFileStream<Object> dataFileStream = new DataFileStream<>(input, new GenericDatumReader<>())) {
            List<Schema.Field> handledFields = dataFileStream.getSchema().getFields().stream()
                    .filter(field -> handleColumns.contains(field.name().toLowerCase()))
                    .toList();
            /*
            Define BlockBuilder based on handledFields(avroTypes) and process avro record,
            if handleFields has a size of 0, add at least 1 field to prevent `blocks is empty` error
            handleFields can be empty when `select count (*)`
             */
            if (handledFields.isEmpty()) {
                handledFields = dataFileStream.getSchema().getFields().stream()
                        .limit(1)
                        .toList();
            }
            List<Type> avroTypes = handledFields.stream()
                    .map(field -> fromAvroType(field.schema()))
                    .toList();

            List<Page> result = new ArrayList<>();
            int batchSize = 1024 * 4; // 4 kb
            boolean hasMoreData = true;

            while (hasMoreData) {
                BlockBuilder[] blockBuilders = new BlockBuilder[avroTypes.size()];
                for (int i = 0; i < avroTypes.size(); i++) {
                    blockBuilders[i] = avroTypes.get(i).createBlockBuilder(null, batchSize);
                }

                int recordCount = 0;
                while (dataFileStream.hasNext() && recordCount < batchSize) {
                    if (dataFileStream.next() instanceof GenericRecord record) {
                        processAvroRecord(record, handledFields, blockBuilders, avroTypes);
                        recordCount++;
                    }

                    if (recordCount > 0) {
                        Block[] blocks = new Block[blockBuilders.length];
                        for (int i = 0; i < blockBuilders.length; i++) {
                            blocks[i] = blockBuilders[i].build();
                        }
                        result.add(new Page(blocks));
                    }
                }

                hasMoreData = dataFileStream.hasNext();
            }
            return result;
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to read Avro file: %s", path), e);
        }
    }

    private void processAvroRecord(GenericRecord record, List<Schema.Field> fields, BlockBuilder[] blockBuilders, List<Type> trinoTypes)
    {
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            Object value = record.get(field.name());
            AvroColumnDecoder.serializeObject(blockBuilders[i], value, trinoTypes.get(i), field.name());
        }
    }
}
