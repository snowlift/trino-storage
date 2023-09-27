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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.VerifyException;
import com.google.common.collect.Streams;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class JsonPlugin
        implements FilePlugin
{
    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(streamProvider.apply(path)))) {
            // check for lines up to 1MB
            reader.mark(1024 * 1024);
            String firstLine = reader.readLine();
            JsonNode node;
            try {
                node = objectMapper.readTree(firstLine);
            }
            catch (JsonProcessingException e) {
                // try to read whole file and assume there's an array of objects, so get the first one
                reader.reset();
                JsonNode root = objectMapper.readTree(reader);
                Iterator<JsonNode> elements = root.elements();
                if (!elements.hasNext()) {
                    return List.of();
                }
                node = elements.next();
            }
            return Streams.stream(node.fields())
                    .map(entry -> new StorageColumnHandle(entry.getKey(), mapType(entry.getValue())))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Type mapType(JsonNode node)
    {
        switch (node.getNodeType()) {
            case ARRAY:
                Iterator<JsonNode> elements = node.elements();
                if (!elements.hasNext()) {
                    throw new VerifyException("Cannot infer the SQL type of an empty JSON array");
                }
                return new ArrayType(mapType(elements.next()));
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case NUMBER:
                return node.canConvertToLong() ? BIGINT : DOUBLE;
            case OBJECT:
                List<RowType.Field> fields = Streams.stream(node.fields())
                        .map(entry -> new RowType.Field(Optional.of(entry.getKey()), mapType(entry.getValue())))
                        .collect(toImmutableList());
                return RowType.from(fields);
            case NULL:
            case STRING:
                return VARCHAR;
            // BINARY, MISSING, POJO
            default:
                throw new VerifyException("Unhandled JSON type: " + node.getNodeType());
        }
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        BufferedReader reader = new BufferedReader(new InputStreamReader(streamProvider.apply(path)));
        try {
            // check for lines up to 1MB
            reader.mark(1024 * 1024);
            String firstLine = reader.readLine();
            try {
                // try parsing the first line only to confirm this is a JSONL file, so throw away the result
                objectMapper.readTree(firstLine);
                reader.reset();
                return reader.lines().map(node -> {
                    try {
                        return nodeToRow(objectMapper.readTree(node));
                    }
                    catch (JsonProcessingException e) {
                        throw new TrinoException(TYPE_MISMATCH, e);
                    }
                });
            }
            catch (JsonProcessingException e) {
                // try to read whole file and assume there's an array of objects, so get the first one
                reader.reset();
                JsonNode root = objectMapper.readTree(reader);
                return Streams.stream(root.elements())
                        .map(this::nodeToRow);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<?> nodeToRow(JsonNode node)
    {
        return Streams.stream(node.fields())
                .map(entry -> mapValue(entry.getValue()))
                .collect(Collectors.toList());
    }

    private Object mapValue(JsonNode node)
    {
        switch (node.getNodeType()) {
            case ARRAY -> {
                Iterator<JsonNode> elements = node.elements();
                if (!elements.hasNext()) {
                    throw new VerifyException("Cannot infer the SQL type of an empty JSON array");
                }
                Type type = mapType(elements.next());
                BlockBuilder values = type.createBlockBuilder(null, 10);
                node.elements().forEachRemaining(value -> writeObject(values, value));
                return values.build();
            }
            case BOOLEAN -> {
                return node.asBoolean();
            }
            case NUMBER -> {
                return node.canConvertToLong() ? node.asLong() : node.asDouble();
            }
            case OBJECT -> {
                Type rowType = mapType(node);
                RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 10);
                rowBlockBuilder.buildEntry(elementBuilder -> {
                    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                    for (int i = 0; fields.hasNext(); i++) {
                        writeObject(elementBuilder.get(i), fields.next().getValue());
                    }
                });
                Block block = rowBlockBuilder.build();
                return rowType.getObject(block, block.getPositionCount() - 1);
            }
            case NULL -> {
                return null;
            }
            case STRING -> {
                return node.asText();
            }
            default ->
                // BINARY, MISSING, POJO
                    throw new VerifyException("Unhandled JSON type: " + node.getNodeType());
        }
    }

    private void writeObject(BlockBuilder blockBuilder, JsonNode node)
    {
        Type type = mapType(node);
        Object value = mapValue(node);
        Class<?> javaType = type.getJavaType();

        if (javaType == long.class) {
            BIGINT.writeLong(blockBuilder, (long) value);
        }
        else if (javaType == double.class) {
            DOUBLE.writeDouble(blockBuilder, (double) value);
        }
        else if (javaType == boolean.class) {
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == Slice.class) {
            VARCHAR.writeString(blockBuilder, (String) value);
        }
        else if (!javaType.isPrimitive()) {
            type.writeObject(blockBuilder, mapValue(node));
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType + " from type " + type);
        }
    }
}
