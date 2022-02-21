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

import io.trino.spi.Page;
import org.ebyhr.trino.storage.StorageColumn;

import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface FilePlugin
{
    List<StorageColumn> getFields(InputStream inputStream);

    default Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        throw new UnsupportedOperationException("A FilePlugin must implement either getRecordsIterator or getPagesIterator");
    }

    default Iterable<Page> getPagesIterator(String path, Function<String, InputStream> streamProvider)
    {
        throw new UnsupportedOperationException("A FilePlugin must implement either getPagesIterator or getRecordsIterator");
    }
}
