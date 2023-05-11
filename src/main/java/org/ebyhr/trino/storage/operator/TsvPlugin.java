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

import com.google.common.base.Splitter;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TsvPlugin
        implements FilePlugin
{
    private static final String DELIMITER = "\t";

    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        Splitter splitter = Splitter.on(DELIMITER).trimResults();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(streamProvider.apply(path)))) {
            List<String> fields = splitter.splitToList(reader.readLine());
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
        InputStream inputStream = streamProvider.apply(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        Splitter splitter = Splitter.on(DELIMITER).trimResults();
        return reader.lines()
                .skip(1)
                .map(splitter::splitToList);
    }
}
