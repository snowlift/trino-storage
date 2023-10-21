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

public final class TestRestrictedStorageConnector
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
                ImmutableMap.of("allow-local-files", "false"));
    }

    @Test
    public void testSelectOrc()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(storage.system.read_file('orc', '" + toAbsolutePath("example-data/apache-lz4.orc") + "')) WHERE x = 1658882660",
                "Reading local files is disabled");
        assertQuery(
                "SELECT * FROM TABLE(storage.system.read_file('orc', '" + toRemotePath("example-data/apache-lz4.orc") + "')) WHERE x = 1658882660",
                "VALUES (1658882660, 639, -5557347160648450358)");
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
