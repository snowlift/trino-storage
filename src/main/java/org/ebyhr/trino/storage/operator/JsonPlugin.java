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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.VerifyException;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class JsonPlugin
        implements FilePlugin
{
    private static final Logger log = Logger.get(JsonPlugin.class);

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
                        .map(entry -> RowType.field(entry.getKey(), mapType(entry.getValue())))
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
                // try parsing the first line to confirm this is a JSONL file
                JsonNode firstNode = objectMapper.readTree(firstLine);
                verify(firstNode instanceof ObjectNode, "JSON file does not contain a list of objects");
                Type type = mapType(firstNode);
                verify(type instanceof RowType, "type is not a RowType");
                reader.reset();
                return reader.lines().map(rawNode -> {
                    try {
                        return nodeToRow(objectMapper.readTree(rawNode), (RowType) type);
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
                if (!root.elements().hasNext()) {
                    return Stream.of();
                }
                Type type = mapType(root.elements().next());
                verify(type instanceof RowType, "type is not a RowType");
                return Streams.stream(root.elements())
                        .map(node -> nodeToRow(node, (RowType) type));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<?> nodeToRow(JsonNode node, RowType type)
    {
        verify(node instanceof ObjectNode, "Expected an object node but got %s".formatted(node.getNodeType()));
        // all rows must have the same type, otherwise Trino will throw unexpected errors when decoding internal value representations
        // type doesn't have to match node so iterate over it and ignore missing fields
        // TODO this silently ignores properties not present in the first row, throw an exception or create a union of all rows
        return type.getFields().stream()
                .map(field -> mapValue(node.get(field.getName().orElseThrow()), field.getType()))
                .collect(Collectors.toList());
    }

    private Object mapValue(JsonNode node, Type type)
    {
        if (node == null) {
            return null;
        }
        Type actualType = mapType(node);
        if (!actualType.equals(type)) {
            // log as debug to avoid spamming logs
            // TODO if the JSON file was converted from XML, it's common to see an object instead of an array - wrap it instead of ignoring it
            log.debug("expected node %s to be of type %s but it is %s, ignoring".formatted(node, mapType(node), type));
            return null;
        }
        switch (node.getNodeType()) {
            case ARRAY -> {
                ArrayType arrayType = (ArrayType) type;
                ArrayBlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, node.size());
                arrayBlockBuilder.buildEntry(elementBuilder -> node.elements().forEachRemaining(value -> writeObject(elementBuilder, value, arrayType.getElementType())));
                Block block = arrayBlockBuilder.build();
                return arrayType.getObject(block, block.getPositionCount() - 1);
            }
            case BOOLEAN -> {
                return node.asBoolean();
            }
            case NUMBER -> {
                return node.canConvertToLong() ? node.asLong() : node.asDouble();
            }
            case OBJECT -> {
                RowType rowType = (RowType) type;
                RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, node.size());
                rowBlockBuilder.buildEntry(fieldBuilders -> {
                    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                    for (int i = 0; fields.hasNext(); i++) {
                        writeObject(fieldBuilders.get(i), fields.next().getValue(), rowType.getFields().get(i).getType());
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

    private void writeObject(BlockBuilder blockBuilder, JsonNode node, Type type)
    {
        if (node.isNull()) {
            blockBuilder.appendNull();
            return;
        }
        Object value = mapValue(node, type);
        verify(value != null, "value is null");
        Class<?> javaType = type.getJavaType();

        if (javaType == long.class) {
            if (value instanceof Double doubleValue) {
                BIGINT.writeLong(blockBuilder, doubleValue.longValue());
            }
            else {
                BIGINT.writeLong(blockBuilder, (long) value);
            }
        }
        else if (javaType == double.class) {
            if (value instanceof Long longValue) {
                DOUBLE.writeDouble(blockBuilder, longValue.doubleValue());
            }
            else {
                DOUBLE.writeDouble(blockBuilder, (double) value);
            }
        }
        else if (javaType == boolean.class) {
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == Slice.class) {
            VARCHAR.writeString(blockBuilder, (String) value);
        }
        else if (!javaType.isPrimitive()) {
            type.writeObject(blockBuilder, value);
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType + " from type " + type);
        }
    }
}
