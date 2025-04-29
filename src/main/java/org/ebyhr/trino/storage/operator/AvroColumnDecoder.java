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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.operator.AvroColumnDecoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;

// copied from io.trino.decoder.avro.AvroColumnDecoder
public class AvroColumnDecoder
{
    private AvroColumnDecoder() {}

    private static Slice getSlice(Object value, Type type, String columnName)
    {
        if (type instanceof VarcharType && (value instanceof CharSequence || value instanceof GenericEnumSymbol)) {
            return truncateToLength(utf8Slice(value.toString()), type);
        }

        if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                return Slices.wrappedHeapBuffer((ByteBuffer) value);
            }
            if (value instanceof GenericFixed) {
                return Slices.wrappedBuffer(((GenericFixed) value).bytes());
            }
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    public static Object serializeObject(BlockBuilder builder, Object value, Type type, String columnName)
    {
        if (type instanceof ArrayType) {
            return serializeList(builder, value, type, columnName);
        }
        if (type instanceof MapType mapType) {
            return serializeMap(builder, value, mapType, columnName);
        }
        if (type instanceof RowType) {
            return serializeRow(builder, value, type, columnName);
        }
        serializePrimitive(builder, value, type, columnName);
        return null;
    }

    private static Block serializeList(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }
        List<?> list = (List<?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type elementType = typeParameters.get(0);

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            serializeObject(blockBuilder, element, elementType, columnName);
        }
        if (parentBlockBuilder != null) {
            type.writeObject(parentBlockBuilder, blockBuilder.build());
            return null;
        }
        return blockBuilder.build();
    }

    private static void serializePrimitive(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        requireNonNull(blockBuilder, "blockBuilder is null");

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, (Boolean) value);
            return;
        }

        if ((value instanceof Integer || value instanceof Long) && (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
            return;
        }

        if (type instanceof DoubleType) {
            type.writeDouble(blockBuilder, (Double) value);
            return;
        }

        if (type instanceof RealType) {
            type.writeLong(blockBuilder, floatToIntBits((Float) value));
            return;
        }

        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            type.writeSlice(blockBuilder, getSlice(value, type, columnName));
            return;
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    private static SqlMap serializeMap(BlockBuilder parentBlockBuilder, Object value, MapType type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        Map<?, ?> map = (Map<?, ?>) value;
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();

        if (parentBlockBuilder != null) {
            ((MapBlockBuilder) parentBlockBuilder).buildEntry((keyBuilder, valueBuilder) -> buildMap(columnName, map, keyType, valueType, keyBuilder, valueBuilder));
            return null;
        }
        return buildMapValue(type, map.size(), (keyBuilder, valueBuilder) -> buildMap(columnName, map, keyType, valueType, keyBuilder, valueBuilder));
    }

    private static void buildMap(String columnName, Map<?, ?> map, Type keyType, Type valueType, BlockBuilder keyBuilder, BlockBuilder valueBuilder)
    {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() != null) {
                keyType.writeSlice(keyBuilder, truncateToLength(utf8Slice(entry.getKey().toString()), keyType));
                serializeObject(valueBuilder, entry.getValue(), valueType, columnName);
            }
        }
    }

    private static SqlRow serializeRow(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(blockBuilder != null, "block builder is null");
            blockBuilder.appendNull();
            return null;
        }

        RowType rowType = (RowType) type;
        if (blockBuilder == null) {
            return buildRowValue(rowType, fieldBuilders -> buildRow(rowType, columnName, (GenericRecord) value, fieldBuilders));
        }

        ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> buildRow(rowType, columnName, (GenericRecord) value, fieldBuilders));
        return null;
    }

    private static void buildRow(RowType type, String columnName, GenericRecord record, List<BlockBuilder> fieldBuilders)
    {
        List<Field> fields = type.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            checkState(field.getName().isPresent(), "field name not found");
            serializeObject(fieldBuilders.get(i), record.get(field.getName().get()), field.getType(), columnName);
        }
    }

    // copied from io.trino.decoder.DecoderErrorCode
    enum DecoderErrorCode
            implements ErrorCodeSupplier
    {
        /**
         * A requested data conversion is not supported.
         */
        DECODER_CONVERSION_NOT_SUPPORTED(0, EXTERNAL);

        private final ErrorCode errorCode;

        DecoderErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x0101_0000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }
}
