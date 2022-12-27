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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.FileStatus;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ListPageSource
        implements ConnectorPageSource
{
    private final long readTimeNanos;
    private final FileStatus[] fileStatuses;
    private boolean done;

    public ListPageSource(StorageClient storageClient, ConnectorSession session, String path)
    {
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

        PageBuilder page = new PageBuilder(fileStatuses.length, ImmutableList.of(BIGINT, BIGINT, VARCHAR));
        for (FileStatus status : fileStatuses) {
            page.declarePosition();
            BIGINT.writeLong(page.getBlockBuilder(0), packDateTimeWithZone(status.getModificationTime(), UTC_KEY));
            BIGINT.writeLong(page.getBlockBuilder(1), status.getLen());
            VARCHAR.writeSlice(page.getBlockBuilder(2), Slices.utf8Slice(status.getPath().getName()));
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
