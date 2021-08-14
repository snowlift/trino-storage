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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.ebyhr.trino.storage.StorageQueryRunner.createStorageQueryRunner;

public final class TestStorageConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createStorageQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testSelectCsv()
    {
        assertQuery(
                format("SELECT * FROM storage.csv.\"%s\"", toAbsolutePath("example-data/numbers-2.csv")),
                "VALUES ('eleven', '11'), ('twelve', '12')");
    }

    @Test
    public void testSelectTsv()
    {
        assertQuery(
                format("SELECT * FROM storage.tsv.\"%s\"", toAbsolutePath("example-data/numbers.tsv")),
                "VALUES ('two', '2'), ('three', '3')");
    }

    @Test
    public void testSelectExcel()
    {
        assertQuery(
                format("SELECT * FROM storage.excel.\"%s\"", toAbsolutePath("example-data/sample.xlsx")),
                "VALUES ('a', '1'), ('b', '2')");
    }

    private static String toAbsolutePath(String resourceName)
    {
        return Resources.getResource(resourceName).toString();
    }
}
