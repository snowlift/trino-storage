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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.metadata.CompressionKind;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    private static final String ORC_CODEC_METRIC_PREFIX = "OrcReaderCompressionFormat_";

    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;
    private final AggregatedMemoryContext memoryContext;
    private final LocalMemoryContext localMemoryContext;
    private final FileFormatDataSourceStats stats;
    private final CompressionKind compressionKind;
    private boolean closed;
    private long completedPositions;

    private Optional<SourcePage> outstandingPage = Optional.empty();

    public OrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            AggregatedMemoryContext memoryContext,
            FileFormatDataSourceStats stats,
            CompressionKind compressionKind)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.localMemoryContext = memoryContext.newLocalMemoryContext(OrcPageSource.class.getSimpleName());
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
    }

    static TrinoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof OrcCorruptionException) {
            return new TrinoException(GENERIC_INTERNAL_ERROR, exception);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page;
        try {
            if (outstandingPage.isPresent()) {
                page = outstandingPage.get();
                outstandingPage = Optional.empty();
                // Mark no bytes consumed by outstandingPage.
                // We can reset it again below if deletedRows loading yields again.
                // In such case the brief period when it is set to 0 will not be observed externally as
                // page source memory usage is only read by engine after call to getNextPage completes.
                localMemoryContext.setBytes(0);
            }
            else {
                page = recordReader.nextPage();
            }
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw handleException(orcDataSource.getId(), e);
        }

        if (page == null) {
            close();
            return null;
        }

        completedPositions += page.getPositionCount();

        return page;
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        Closer closer = Closer.create();

        closer.register(() -> {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        });

        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcReader", recordReader)
                .toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryContext.getBytes();
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(ORC_CODEC_METRIC_PREFIX + compressionKind.name(), new LongCount(recordReader.getTotalDataLength())));
    }
}
