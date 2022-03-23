package org.ebyhr.trino.storage.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import io.trino.spi.Page;
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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.ebyhr.trino.storage.StorageColumn;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.file.DataFileStream;

import static com.google.shaded.common.shaded.base.Verify.verify;
import static com.google.shaded.common.shaded.collect.ImmutableList.toImmutableList;
import static java.util.Objects.isNull;

public class AvroPlugin implements FilePlugin
{
    @Override
    public List<StorageColumn> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try {
            final DataFileStream<GenericRecord> fileStream = new DataFileStream<>(streamProvider.apply(path), new GenericDatumReader<>());
            return this.columnsFromRecord(fileStream.getSchema());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        try {
            final DataFileStream<GenericRecord> fileStream = new DataFileStream<>(streamProvider.apply(path), new GenericDatumReader<>());
            return StreamSupport
                    .stream(fileStream.spliterator(), false)
                    .map(genericRecord -> {
                        return this.columnsForRecord(genericRecord);
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<?> columnsForRecord(final GenericRecord record) {
        return record.getSchema().getFields().stream()
                .map(field ->
                {
                    return this.toNativeEncoding(field.schema(), record.get(field.name()));
                }).collect(toImmutableList());
    }

    private Object toNativeEncoding(final Schema schema, final Object avroEncoding) {
        if(isNull(avroEncoding)) return null;
        try {
            switch (schema.getType()) {
                case BOOLEAN:
                case INT:
                case LONG:
                case DOUBLE:
                        return (typeFromAvro(schema).getJavaType()).cast(avroEncoding);
                    break;
                case RECORD:
                    return new Block
                    break;
                case ENUM:
                case STRING:

                    break;
                case ARRAY:
                    break;
                case MAP:
                    break;
                case UNION:
                    break;
                case FIXED:
                    break;
                case STRING:
                    break;
                case BYTES:
                    break;
                case FLOAT:
                    return (long) Float.floatToIntBits((Float) avroEncoding);
                case NULL:
                default:
                    throw new IllegalStateException("Unexpected value: " + schema.getType());
            }
        } catch (ClassCastException e) {
            throw new IllegalStateException(String.format("Unexpected native avro encoding of %s for schema type %s", avroEncoding.getClass().toString(), schema.getType().name()));
        }
    }

    @VisibleForTesting
    private List<StorageColumn> columnsFromRecord(final Schema schema) {
        verify(schema.getType() == Schema.Type.RECORD, "Can only get columns from a record schema type. Found schema type: %s", schema.getType().name());
        return schema.getFields().stream()
                .map(field -> new StorageColumn(field.name(), typeFromAvro(field.schema())))
                .collect(toImmutableList());
    }

    private Type typeFromAvro(final Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return RowType.from(schema.getFields()
                        .stream()
                        .map(field ->
                        {
                            return new RowType.Field(Optional.of(field.name()), this.typeFromAvro(field.schema()));
                        }).collect(toImmutableList()));
            case ENUM:
                return VarcharType.VARCHAR;
            case ARRAY:
                return new ArrayType(this.typeFromAvro(schema.getElementType()));
            case MAP:
                return new MapType(VarcharType.VARCHAR, this.typeFromAvro(schema.getValueType()), new TypeOperators());
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
}
