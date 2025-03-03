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
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public final class ParquetTypeTranslator
{
    private ParquetTypeTranslator() {}

    public static Type fromParquetType(org.apache.parquet.schema.Type type)
    {
        if (type.isPrimitive()) {
            return fromParquetType(type.asPrimitiveType());
        }
        return fromParquetType(type.asGroupType());
    }

    public static Type fromParquetType(GroupType groupType)
    {
        LogicalTypeAnnotation logicalTypeAnnotation = groupType.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            Type elementType = fromParquetType(groupType.getType(0));
            return new ArrayType(elementType);
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            Type keyType = fromParquetType(groupType.getType(0));
            Type elementType = fromParquetType(groupType.getType(1));
            return new MapType(keyType, elementType, new TypeOperators());
        }
        ImmutableList.Builder<Type> fieldTypeInfo = ImmutableList.builder();
        groupType.getFields().forEach(groupField -> fieldTypeInfo.add(fromParquetType(groupField)));
        return RowType.anonymous(fieldTypeInfo.build());
    }

    public static Type fromParquetType(PrimitiveType parquetType)
    {
        LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logicalTypeAnnotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
            return VarcharType.VARCHAR;
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return DateType.DATE;
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return DecimalType.createDecimalType(decimalLogicalTypeAnnotation.getPrecision(), decimalLogicalTypeAnnotation.getScale());
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalTypeAnnotation = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation;
            switch (timeLogicalTypeAnnotation.getUnit()) {
                case MICROS:
                    return TimeType.TIME_MICROS;
                case MILLIS:
                    return TimeType.TIME_MILLIS;
                case NANOS:
                    return TimeType.TIME_NANOS;
            }
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + logicalTypeAnnotation);
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalTypeAnnotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
            switch (timestampLogicalTypeAnnotation.getUnit()) {
                case MICROS:
                    return TimestampType.TIMESTAMP_MICROS;
                case MILLIS:
                    return TimestampType.TIMESTAMP_MILLIS;
                case NANOS:
                    return TimestampType.TIMESTAMP_NANOS;
            }
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + logicalTypeAnnotation);
        }
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
            return UuidType.UUID;
        }
        // fall back to checking primitive types
        switch (parquetType.getPrimitiveTypeName()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case INT96:
                return TimestampType.TIMESTAMP_NANOS;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BINARY:
                return VarbinaryType.VARBINARY;
            case FIXED_LEN_BYTE_ARRAY:
                // UUID should be handled by logical type annotations, otherwise unsupported
                break;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column: " + parquetType.getPrimitiveTypeName());
    }
}
