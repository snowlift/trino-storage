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

import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestStorageClient
{
    @Test
    public void testMetadata()
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        StorageClient client = new StorageClient(hdfsEnvironment);
        assertEquals(client.getSchemaNames(), List.of("csv", "tsv", "txt", "raw", "excel", "orc", "json"));
    }
}
