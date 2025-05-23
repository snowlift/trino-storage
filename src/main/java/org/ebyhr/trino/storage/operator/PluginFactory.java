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

import io.trino.spi.connector.SchemaNotFoundException;

import static java.util.Locale.ENGLISH;

public final class PluginFactory
{
    private PluginFactory() {}

    public static FilePlugin create(String typeName)
    {
        switch (typeName.toLowerCase(ENGLISH)) {
            case "csv":
                return new CsvPlugin(',');
            case "tsv":
                return new CsvPlugin('\t');
            case "ssv":
                return new CsvPlugin(';');
            case "txt":
                return new TextPlugin();
            case "raw":
                return new RawPlugin();
            case "excel":
                return new ExcelPlugin();
            case "orc":
                return new OrcPlugin();
            case "parquet":
                return new ParquetPlugin();
            case "json":
                return new JsonPlugin();
            case "avro":
                return new AvroPlugin();
            default:
                throw new SchemaNotFoundException(typeName);
        }
    }
}
