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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestFlexRecordSet
{
    private static final URI CSV = URI.create("https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers-2.csv");
    private static final URI TSV = URI.create("https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv");

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", BIGINT, 1)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(createUnboundedVarcharType(), BIGINT));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", BIGINT, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), BIGINT);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    @Test
    public void testTsvCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "tsv", TSV.toString(), TSV), ImmutableList.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, ImmutableMap.<String, String>builder()
                .put("two", "2")
                .put("three", "3")
                .build());
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", "table", CSV), ImmutableList.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        RecordCursor cursor = recordSet.cursor();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertEquals(cursor.getLong(0), cursor.getLong(1));
            data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }
}
