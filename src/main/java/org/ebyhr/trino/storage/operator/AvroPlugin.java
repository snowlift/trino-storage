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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.ebyhr.trino.storage.StorageColumn;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.shaded.common.shaded.base.Verify.verify;
import static com.google.shaded.common.shaded.collect.ImmutableList.toImmutableList;

public class AvroPlugin
        implements FilePlugin
{
    private static Type typeFromAvro(final Schema schema)
    {
        switch (schema.getType()) {
            case RECORD:
                return RowType.from(schema.getFields()
                        .stream()
                        .map(field ->
                        {
                            return new RowType.Field(Optional.of(field.name()), typeFromAvro(field.schema()));
                        }).collect(toImmutableList()));
            case ENUM:
                return VarcharType.VARCHAR;
            case ARRAY:
                return new ArrayType(typeFromAvro(schema.getElementType()));
            case MAP:
                return new MapType(VarcharType.VARCHAR, typeFromAvro(schema.getValueType()), new TypeOperators());
            case UNION:
                throw new UnsupportedOperationException("No union type support for now");
            case FIXED:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case NULL:
                throw new UnsupportedOperationException("No null column type support");
            default:
                throw new VerifyException("Schema type unknown: " + schema.getType().toString());
        }
    }

    private static AvroBlockBuilder getAvroBlockBuilderForSchema(Function<GenericRecord, Object> objectAccessor, Schema schema)
    {
        switch (schema.getType()) {
            case ENUM:
            case FIXED:
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return new SimpleAvroBlockBuilder(objectAccessor, schema);
            case RECORD:
                return new RowAvroBlockBuilder(objectAccessor, schema);
            default:
                throw new IllegalStateException("Not Avro Block Builder for: " + schema.getType());
        }
    }

//    @Override
//    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
//    {
//        try {
//            final DataFileStream<GenericRecord> fileStream = new DataFileStream<>(streamProvider.apply(path), new GenericDatumReader<>());
//            return StreamSupport
//                    .stream(fileStream.spliterator(), false)
//                    .map(this::columnsForRecord);
//        }
//        catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }
//
//    private List<?> columnsForRecord(final GenericRecord record) {
//        return record.getSchema().getFields().stream()
//                .map(field ->
//                {
//                    return this.toNativeEncoding(field.schema(), record.get(field.name()));
//                }).collect(toImmutableList());
//    }
//
//    private Object toNativeEncoding(final Schema schema, final Object avroEncoding) {
//        if(isNull(avroEncoding)) return null;
//        Function<GenericRecord, Object> f = null;
//        try {
//            switch (schema.getType()) {
//                case BOOLEAN:
//                case INT:
//                case LONG:
//                case DOUBLE:
//                        return (typeFromAvro(schema).getJavaType()).cast(avroEncoding);
//                    break;
//                case FLOAT:
//                    return (long) Float.floatToIntBits((Float) avroEncoding);
//                case RECORD:
//                    throw new IllegalStateException("Not supported value: " + schema.getType());
//                    break;
//                case ENUM:
//                case STRING:
//                    return ((String) avroEncoding);
//                case ARRAY:
//                    Type innerType = typeFromAvro(schema.getElementType());
//                    BlockBuilder builder = typeFromAvro(schema).createBlockBuilder(null, ((List) avroEncoding).size());
//                    ((List<Object>) avroEncoding).forEach(innerObj -> {
//                        innerType.writeObject(builder, this.toNativeEncoding(schema.getElementType(), innerObj));
//                    });
//                    return builder.build();
//                case MAP:
//                    break;
//                case UNION:
//                    break;
//                case FIXED:
//                    break;
//                case BYTES:
//                    break;
//                case NULL:
//                default:
//                    throw new IllegalStateException("Unexpected value: " + schema.getType());
//            }
//        } catch (ClassCastException e) {
//            throw new IllegalStateException(String.format("Unexpected native avro encoding of %s for schema type %s", avroEncoding.getClass().toString(), schema.getType().name()));
//        }
//    }

    @Override
    public List<StorageColumn> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try {
            final DataFileStream<GenericRecord> fileStream = new DataFileStream<>(streamProvider.apply(path), new GenericDatumReader<>());
            return this.columnHandlesFromSchema(fileStream.getSchema());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider)
    {
        try {
            final DataFileStream<GenericRecord> fileStream = new DataFileStream<>(streamProvider.apply(path), new GenericDatumReader<>());
            PageAvroBlockBuilder builder = new PageAvroBlockBuilder(a -> a, fileStream.getSchema());
            for (GenericRecord record : fileStream) {
                builder.append(record);
            }
            return List.of(builder.buildPage());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    private List<StorageColumn> columnHandlesFromSchema(final Schema schema)
    {
        verify(schema.getType() == Schema.Type.RECORD, "Can only get columns from a record schema type. Found schema type: %s", schema.getType().name());
        return schema.getFields().stream()
                .map(field -> new StorageColumn(field.name(), typeFromAvro(field.schema())))
                .collect(toImmutableList());
    }

    private interface AvroBlockBuilder
    {
        void append(GenericRecord record);

        Block build();
    }

    private static class SimpleAvroBlockBuilder
            implements AvroBlockBuilder
    {
        private final Schema schema;
        private final Type simpleType;
        private final Function<GenericRecord, Object> objectAccessor;
        BlockBuilder builder;

        SimpleAvroBlockBuilder(Function<GenericRecord, Object> objectAccessor, Schema schema)
        {
            this.objectAccessor = objectAccessor;
            this.schema = schema;
            switch (schema.getType()) {
                case ENUM:
                case FIXED:
                case STRING:
                case BYTES:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    this.simpleType = typeFromAvro(schema);
                    this.builder = typeFromAvro(schema).createBlockBuilder(null, 10);
                break;
                default:
                    throw new IllegalStateException("Unable to make SimpleAvroBlockBuilder for schema type " + schema.getType());
            }
        }

        @Override
        public void append(GenericRecord record)
        {
            try {
                Object object = this.objectAccessor.apply(record);
                if (Objects.isNull(object)) {
                    this.builder.appendNull();
                }
                switch (schema.getType()) {
                    case FIXED:
                    case BYTES:
                        throw new IllegalStateException("I have no idea the class here " + object.getClass().toString());
                    case ENUM:
                    case STRING:
                        ((VarcharType) this.simpleType).writeString(this.builder, object.toString());
                        break;
                    case INT:
                    case LONG:
                        this.simpleType.writeLong(this.builder, ((Number) object).longValue());
                        break;
                    case FLOAT:
                        this.simpleType.writeLong(this.builder, Float.floatToIntBits((float) object));
                        break;
                    case DOUBLE:
                        this.simpleType.writeDouble(this.builder, (double) object);
                        break;
                    case BOOLEAN:
                        this.simpleType.writeBoolean(this.builder, (boolean) object);
                        break;
                    default:
                        throw new IllegalStateException("Schema skew added in constructor not supported in append" + schema.getType());
                }
            }
            catch (ClassCastException e) {
                throw new ClassCastException(String.format("Unable to cast object %s with class %s using the expected type by the schema %s", this.objectAccessor.apply(record), this.objectAccessor.apply(record).getClass(), this.schema));
            }
        }

        @Override
        public Block build()
        {
            return this.builder.build();
        }
    }

    private static class ArrayAvroBlockBuilder
            implements AvroBlockBuilder
    {
        ArrayBlockBuilder builder;
        private Schema schema;
        private Type type;
        private Function<GenericRecord, Object> objectAccessor;

        ArrayAvroBlockBuilder(Function<GenericRecord, Object> objectAccessor, Schema schema)
        {
            this.schema = schema;
            this.objectAccessor = objectAccessor;
            switch (schema.getType()) {
                case ARRAY:
                    this.type = typeFromAvro(schema);
                    this.builder = (ArrayBlockBuilder) this.type.createBlockBuilder(null, 10);
                default:
                    throw new IllegalStateException("Unable to make SimpleAvroBlockBuilder for schema type " + schema.getType());
            }
        }

        @Override
        public void append(GenericRecord record)
        {
            try {
                Object object = this.objectAccessor.apply(record);
                if (Objects.isNull(object)) {
                    this.builder.appendNull();
                }

                switch (schema.getType()) {
                    case ARRAY:
                    default:
                        throw new IllegalStateException("Schema skew added in constructor not supported in append" + schema.getType());
                }
            }
            catch (ClassCastException e) {
                throw new ClassCastException(String.format("Unable to cast object %s using the expected type by the schema %s", this.objectAccessor.apply(record), this.schema));
            }
        }

        @Override
        public Block build()
        {
            return this.builder.build();
        }
    }

    private static class RowAvroBlockBuilder
            implements AvroBlockBuilder
    {
        private final Function<GenericRecord, Object> objectAccessor;
        private final ArrayList<AvroBlockBuilder> fieldBuilders = new ArrayList<>();

        RowAvroBlockBuilder(Function<GenericRecord, Object> objectAccessor, Schema schema)
        {
            this.objectAccessor = objectAccessor;
            if (schema.getType() == Schema.Type.RECORD) {
                for (Schema.Field field : schema.getFields()) {
                    final String fieldName = field.name();
                    Function<GenericRecord, Object> fieldAccessor = this.objectAccessor.andThen(obj -> {
                        GenericRecord record = (GenericRecord) obj;
                        return record.get(fieldName);
                    });
                    fieldBuilders.add(getAvroBlockBuilderForSchema(fieldAccessor, field.schema()));
                }
            }
            else {
                throw new IllegalStateException("Unable to make SimpleAvroBlockBuilder for schema type " + schema.getType());
            }
        }

        @Override
        public void append(GenericRecord record)
        {
            for (AvroBlockBuilder builder : this.fieldBuilders) {
                builder.append(record);
            }
        }

        protected Block[] getBuiltBlocks()
        {
            return this.fieldBuilders.stream().map(AvroBlockBuilder::build).toArray(Block[]::new);
        }

        @Override
        public Block build()
        {
            Block[] blocks = this.getBuiltBlocks();
            return RowBlock.fromFieldBlocks(blocks.length, Optional.empty(), blocks);
        }
    }

    private static class PageAvroBlockBuilder
            extends RowAvroBlockBuilder
    {
        PageAvroBlockBuilder(Function<GenericRecord, Object> objectAccessor, Schema schema)
        {
            super(objectAccessor, schema);
        }

        Page buildPage()
        {
            Block[] blocks = this.getBuiltBlocks();
            return new Page(blocks);
        }
    }
}
