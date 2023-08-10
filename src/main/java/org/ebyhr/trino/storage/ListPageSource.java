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
package org.ebyhr.trino.storage;

import io.airlift.slice.Slices;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ListPageSource
        implements ConnectorPageSource
{
    private final List<? extends ColumnHandle> columns;
    private final long readTimeNanos;
    private final FileIterator fileStatuses;
    private boolean done;

    public ListPageSource(StorageClient storageClient, ConnectorSession session, String path, List<? extends ColumnHandle> columns)
    {
        this.columns = columns;
        long start = System.nanoTime();
        this.fileStatuses = storageClient.list(session, path);
        readTimeNanos = System.nanoTime() - start;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }

    @Override
    public Page getNextPage()
    {
        if (done) {
            return null;
        }

        done = true;

        PageBuilder page = new PageBuilder(columns.stream().map(column -> ((StorageColumnHandle) column).getType()).toList());
        try {
            while (fileStatuses.hasNext()) {
                FileEntry status = fileStatuses.next();
                page.declarePosition();
                for (int i = 0; i < columns.size(); i++) {
                    StorageColumnHandle column = (StorageColumnHandle) columns.get(i);
                    switch (column.getName()) {
                        case "file_modified_time" -> BIGINT.writeLong(page.getBlockBuilder(i), packDateTimeWithZone(status.lastModified().toEpochMilli(), UTC_KEY));
                        case "size" -> BIGINT.writeLong(page.getBlockBuilder(i), status.length());
                        case "name" -> VARCHAR.writeSlice(page.getBlockBuilder(i), Slices.utf8Slice(status.location().toString()));
                        default -> throw new IllegalStateException("Unknown column name " + column.getName());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return page.build();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
