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

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static org.ebyhr.presto.flex.MetadataUtil.COLUMN_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestFlexColumnHandle
{
    private final FlexColumnHandle columnHandle = new FlexColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 0);

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        FlexColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new FlexColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorId", "columnName", BIGINT, 0),
                        new FlexColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 1))
                .addEquivalentGroup(
                        new FlexColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorIdX", "columnName", BIGINT, 0),
                        new FlexColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 1))
                .addEquivalentGroup(
                        new FlexColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 0),
                        new FlexColumnHandle("connectorId", "columnNameX", BIGINT, 0),
                        new FlexColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 1))
                .check();
    }
}
