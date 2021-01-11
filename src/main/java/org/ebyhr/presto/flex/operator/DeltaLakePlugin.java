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
package org.ebyhr.presto.flex.operator;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import io.delta.tables.DeltaTable;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ebyhr.presto.flex.FlexColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.ebyhr.presto.flex.FileType.TXT;

public class DeltaLakePlugin
        implements FilePlugin
{
    private static final String DELIMITER = ",";

    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        DeltaTable log = DeltaTable.forPath("/tmp/delta-table");
        Dataset<Row> row = log.history();
        System.out.println(row.columns());

        Splitter splitter = Splitter.on(DELIMITER).trimResults();

        ByteSource byteSource;
        try {
            byteSource = Resources.asByteSource(URI.create(table).toURL());
        }
        catch (IllegalArgumentException | MalformedURLException e) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }

        if (schema.equalsIgnoreCase(TXT.toString())) {
            return List.of(new FlexColumn("value", VARCHAR));
        }

        List<FlexColumn> columnTypes = new LinkedList<>();
        try {
            ImmutableList<String> lines = byteSource.asCharSource(UTF_8).readLines();
            List<String> fields = splitter.splitToList(lines.get(0));
            fields.forEach(field -> columnTypes.add(new FlexColumn(field, VARCHAR)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return columnTypes;
    }

    @Override
    public Iterator<String> getIterator(ByteSource byteSource)
    {
        try {
            return byteSource.asCharSource(UTF_8).readLines().iterator();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get iterator");
        }
    }

    @Override
    public List<String> splitToList(Iterator lines)
    {
        String line = (String) lines.next();
        Splitter splitter = Splitter.on(DELIMITER).trimResults();
        return splitter.splitToList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
