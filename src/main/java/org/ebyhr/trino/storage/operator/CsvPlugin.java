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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.Streams;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class CsvPlugin
        implements FilePlugin
{
    private final String delimiter;
    private final CsvMapper mapper;
    private final CsvSchema schema;

    public CsvPlugin(String delimiter)
    {
        this.delimiter = delimiter;
        this.mapper = new CsvMapper();
        this.mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY).enable(CsvParser.Feature.TRIM_SPACES);
        this.schema = CsvSchema.emptySchema().withColumnSeparator(this.delimiter.charAt(0));
    }

    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try {
            // Read the first line and use the values as column names
            MappingIterator<List<String>> it = this.mapper.readerFor(List.class).with(schema).readValues(streamProvider.apply(path));
            List<String> fields = it.next();
            return fields.stream()
                    .map(field -> new StorageColumnHandle(field, VARCHAR))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        try {
            // Read lines and skip the first one because that contains the column names
            MappingIterator<List<?>> it = this.mapper.readerFor(List.class).with(schema).readValues(streamProvider.apply(path));
            return Streams.stream(it).skip(1);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
