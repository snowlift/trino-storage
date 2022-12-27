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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.ebyhr.trino.storage.StorageSplit.Mode.TABLE;
import static org.testng.Assert.assertEquals;

public class TestStorageTableHandle
{
    private final StorageTableHandle tableHandle = new StorageTableHandle(TABLE, "connectorId", "schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<StorageTableHandle> codec = jsonCodec(StorageTableHandle.class);
        String json = codec.toJson(tableHandle);
        StorageTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new StorageTableHandle(TABLE, "connector", "schema", "table"), new StorageTableHandle(TABLE, "connector", "schema", "table"))
                .addEquivalentGroup(new StorageTableHandle(TABLE, "connectorX", "schema", "table"), new StorageTableHandle(TABLE, "connectorX", "schema", "table"))
                .addEquivalentGroup(new StorageTableHandle(TABLE, "connector", "schemaX", "table"), new StorageTableHandle(TABLE, "connector", "schemaX", "table"))
                .addEquivalentGroup(new StorageTableHandle(TABLE, "connector", "schema", "tableX"), new StorageTableHandle(TABLE, "connector", "schema", "tableX"))
                .check();
    }
}
