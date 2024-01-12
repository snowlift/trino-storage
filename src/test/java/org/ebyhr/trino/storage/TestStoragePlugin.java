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

import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestStoragePlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        // simplest possible configuration
        factory.create("test", Map.of(), new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testHttpClient()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test", Map.of("http-client.http-proxy", "http://example.com:8080"), new TestingConnectorContext()).shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new StoragePlugin().getConnectorFactories());
    }
}
