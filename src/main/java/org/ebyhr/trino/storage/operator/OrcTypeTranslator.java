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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Type fromOrcType(OrcType orcType, ColumnMetadata<OrcType> columnMetadata)
    {
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;

            case FLOAT:
            case DOUBLE:
                return DoubleType.DOUBLE;

            case BYTE:
                return VarbinaryType.VARBINARY;

            case DATE:
                return DateType.DATE;

            case SHORT:
            case INT:
                return IntegerType.INTEGER;

            case LONG:
                return BigintType.BIGINT;

            case DECIMAL:
                checkArgument(orcType.getPrecision().isPresent(), "orcType.getPrecision() is not present");
                checkArgument(orcType.getScale().isPresent(), "orcType.getScale() is not present");
                return DecimalType.createDecimalType(orcType.getPrecision().get(), orcType.getScale().get());

            case TIMESTAMP:
                return TimestampType.createTimestampType(orcType.getPrecision().orElse(3));

            case BINARY:
                return VarbinaryType.VARBINARY;

            case CHAR:
            case VARCHAR:
            case STRING:
                return VarcharType.VARCHAR;

            case LIST: {
                Type elementType = fromOrcType(columnMetadata.get(orcType.getFieldTypeIndex(0)), columnMetadata);
                return new ArrayType(elementType);
            }

            case MAP: {
                Type keyType = getType(orcType, 0, columnMetadata);
                Type elementType = getType(orcType, 1, columnMetadata);
                return new MapType(keyType, elementType, new TypeOperators());
            }

            case STRUCT: {
                ImmutableList.Builder<Type> fieldTypeInfo = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    fieldTypeInfo.add(getType(orcType, fieldId, columnMetadata));
                }
                return RowType.anonymous(fieldTypeInfo.build());
            }

            case TIMESTAMP_INSTANT:
            case UNION:
                // unsupported
                break;
        }
        throw new VerifyException("Unhandled ORC type: " + orcType.getOrcTypeKind());
    }

    private static Type getType(OrcType orcType, int index, ColumnMetadata<OrcType> columnMetadata)
    {
        return fromOrcType(columnMetadata.get(orcType.getFieldTypeIndex(index)), columnMetadata);
    }
}
