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

import java.util.Optional;

import static java.lang.String.format;
import static org.ebyhr.trino.storage.StorageQueryRunner.createStorageQueryRunner;

public final class TestStorageConnector
        extends AbstractTestQueryFramework
{
    private TestingStorageServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingStorageServer());
        return createStorageQueryRunner(
                Optional.of(server),
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testSelectCsv()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('csv', '" + toAbsolutePath("example-data/numbers-2.csv") + "'))",
                "VALUES ('eleven', '11'), ('twelve', '12')");
    }

    @Test
    public void testSelectSsv()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('ssv', '" + toAbsolutePath("example-data/numbers-2.ssv") + "'))",
                "VALUES ('eleven', '11'), ('twelve', '12')");
    }

    @Test
    public void testSelectTsv()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('tsv', '" + toAbsolutePath("example-data/numbers.tsv") + "'))",
                "VALUES ('two', '2'), ('three', '3')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('tsv', '" + server.getHadoopServer().toHdfsPath("/tmp/numbers.tsv") + "'))",
                "VALUES ('two', '2'), ('three', '3')");
    }

    @Test
    public void testSelectExcel()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('excel', '" + toAbsolutePath("example-data/sample.xlsx") + "'))",
                "VALUES ('a', '1'), ('b', '2')");
    }

    @Test
    public void testSelectOrc()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('orc', '" + toAbsolutePath("example-data/apache-lz4.orc") + "')) WHERE x = 1658882660",
                "VALUES (1658882660, 639, -5557347160648450358)");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('orc', '" + toRemotePath("example-data/apache-lz4.orc") + "')) WHERE x = 1658882660",
                "VALUES (1658882660, 639, -5557347160648450358)");
    }

    @Test
    public void testSelectJson()
    {
        // note that empty arrays are not supported at all, because array types are inferred from the first array element
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('json', '" + toAbsolutePath("example-data/newlines.json") + "'))",
                "VALUES " +
                        "(true, CAST(null AS VARCHAR), 'aaa', 5, CAST(123.456 AS double), ARRAY['aaa', 'bbb']), " +
                        "(false, CAST(null AS VARCHAR), 'bbb', 10, CAST(123.456 AS double), ARRAY['ccc'])");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('json', '" + toAbsolutePath("example-data/array-of-objects.json") + "'))",
                "VALUES " +
                        "(true, CAST(null AS VARCHAR), 'aaa', 5, CAST(123.456 AS double), ARRAY['aaa', 'bbb']), " +
                        "(false, CAST(null AS VARCHAR), 'bbb', 10, CAST(123.456 AS double), ARRAY['ccc'])");
    }

    private static String toAbsolutePath(String resourceName)
    {
        return Resources.getResource(resourceName).toString();
    }

    private static String toRemotePath(String resourceName)
    {
        return format("https://github.com/snowlift/trino-storage/raw/4c381eca1fa44b22372300659a937a57550c90b9/src/test/resources/%s", resourceName);
    }
}
