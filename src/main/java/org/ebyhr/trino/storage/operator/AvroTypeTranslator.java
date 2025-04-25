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

import java.util.Optional;

public class AvroTypeTranslator
{
    private AvroTypeTranslator() {}

    // ref: AvroSchemaConverter convert
    public static Type fromAvroType(Schema schema)
    {
        switch (schema.getType()) {
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case ENUM:
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
            case FIXED:
                return VarbinaryType.VARBINARY;
            case ARRAY:
                return new ArrayType(fromAvroType(schema.getElementType()));
            case MAP:
                return new MapType(VarcharType.VARCHAR, fromAvroType(schema.getValueType()), new TypeOperators());
            case RECORD:
                return RowType.from(
                        schema.getFields().stream()
                                .map(field -> new RowType.Field(Optional.of(field.name()), fromAvroType(field.schema())))
                                .toList());
            case UNION:
                Schema actualSchema = extractActualTypeFromUnion(schema);
                if (actualSchema != null) {
                    return fromAvroType(actualSchema);
                }
                break;
            case NULL:
                break;
        }

        throw new VerifyException("Unsupported Avro type: " + schema.getType());
    }

    private static Schema extractActualTypeFromUnion(Schema unionSchema)
    {
        // Extract the first non-null type from the union
        return unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(null);
    }
}
