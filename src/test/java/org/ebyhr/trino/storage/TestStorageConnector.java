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
import org.junit.jupiter.api.Test;

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
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('csv', '" + toAbsolutePath("example-data/quoted_fields_with_separator.csv") + "'))",
                "VALUES ('test','2','3','4'),('test,test,test,test','3','3','5'),(' even weirder, but still valid, value with extra whitespaces that remain due to quoting /  ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('csv', '" + toAbsolutePath("example-data/quoted_fields_with_newlines.csv") + "'))",
                "VALUES ('test','2','3','4'),('test,test,test,test','3','3','5'),(' even weirder, but still valid, value with linebreaks and extra\n" +
                        "whitespaces that should remain due to quoting   ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
    }

    @Test
    public void testSelectSsv()
    {
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('ssv', '" + toAbsolutePath("example-data/numbers-2.ssv") + "'))",
                "VALUES ('eleven', '11'), ('twelve', '12')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('ssv', '" + toAbsolutePath("example-data/quoted_fields_with_separator.ssv") + "'))",
                "VALUES ('test','2','3','4'),('test;test;test;test','3','3','5'),(' even weirder; but still valid; value with extra whitespaces that remain due to quoting /  ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('ssv', '" + toAbsolutePath("example-data/quoted_fields_with_newlines.ssv") + "'))",
                "VALUES ('test','2','3','4'),('test;test;test;test','3','3','5'),(' even weirder, but still valid; value with linebreaks and extra\n" +
                        " whitespaces that should remain due to quoting   ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
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
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('tsv', '" + toAbsolutePath("example-data/quoted_fields_with_separator.tsv") + "'))",
                "VALUES ('test','2','3','4'),('test\ttest\ttest\ttest','3','3','5'),(' even weirder\t but still valid\t value with extra whitespaces that remain due to quoting /  ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('tsv', '" + toAbsolutePath("example-data/quoted_fields_with_newlines.tsv") + "'))",
                "VALUES ('test','2','3','4'),('test\ttest\ttest\ttest','3','3','5'),(' even weirder, but still valid, value with linebreaks and extra\n" +
                        " whitespaces that should remain due to quoting   ','1','2','3'),('extra whitespaces that should get trimmed due to no quoting','1','2','3')");
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

    @Test
    public void testList()
    {
        assertQuery(
                "SELECT substr(name, strpos(name, '/', -1) + 1) FROM TABLE(storage.system.list('" + server.getHadoopServer().toHdfsPath("/tmp/") + "')) WHERE name LIKE '%numbers%'",
                "VALUES ('numbers.tsv')");
        assertQuery(
                "SELECT substr(name, strpos(name, '/', -1) + 1) FROM TABLE(storage.system.list('" + toAbsolutePath("example-data/") + "')) WHERE name LIKE '%numbers__.csv'",
                "VALUES ('numbers-1.csv'), ('numbers-2.csv')");
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
