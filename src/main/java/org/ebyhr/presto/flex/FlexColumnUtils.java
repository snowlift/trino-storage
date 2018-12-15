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
package org.ebyhr.presto.flex;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

class FlexColumnUtils
{
    private FlexColumnUtils()
    {
    }

    public static List<FlexColumn> typeOf(String schemaName, String tableName)
    {
        String delimiter = delimiterFromExtension(schemaName);
        Splitter splitter = Splitter.on(delimiter).trimResults();

        List<FlexColumn> columnTypes = new LinkedList<>();
        ByteSource byteSource;
        try {
            byteSource = Resources.asByteSource(URI.create(tableName).toURL());
        }
        catch (IllegalArgumentException | MalformedURLException e) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
        }

        try {
            ImmutableList<String> lines = byteSource.asCharSource(UTF_8).readLines();
            List<String> fields = splitter.splitToList(lines.get(0));
            List<String> data = splitter.splitToList(lines.get(1));

            for (int i = 0; i < fields.size(); i++) {
                Type type = typeOf(data.get(i));
                columnTypes.add(new FlexColumn(fields.get(i), type));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return columnTypes;
    }

    private static String delimiterFromExtension(String type)
    {
        return type.equalsIgnoreCase("csv") ? "," : "\t";
    }

    private static Type typeOf(String value)
    {
        return VARCHAR;
//        return StringUtils.isNumeric(value) ? BIGINT : VARCHAR;
    }
}
