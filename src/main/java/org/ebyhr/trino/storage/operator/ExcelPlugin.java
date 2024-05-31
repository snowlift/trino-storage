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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.ebyhr.trino.storage.StorageColumnHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class ExcelPlugin
        implements FilePlugin
{
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();

    @Override
    public List<StorageColumnHandle> getFields(String path, Function<String, InputStream> streamProvider)
    {
        try (InputStream inputStream = streamProvider.apply(path);
             Workbook workbook = WorkbookFactory.create(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rows = sheet.iterator();
            List<StorageColumnHandle> columnTypes = new LinkedList<>();
            Row row = rows.next();
            for (Cell cell : row) {
                String cellValue = DATA_FORMATTER.formatCellValue(cell);
                columnTypes.add(new StorageColumnHandle(cellValue, VARCHAR));
            }
            return columnTypes;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        try (InputStream inputStream = streamProvider.apply(path);
             Workbook workbook = WorkbookFactory.create(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            Spliterator<Row> spliterator = Spliterators.spliteratorUnknownSize(sheet.iterator(), 0);
            return StreamSupport.stream(spliterator, false)
                    .skip(1)
                    .map(this::splitToList);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<?> splitToList(Row row)
    {
        List<String> values = new ArrayList<>();
        for (Cell cell : row) {
            String cellValue = DATA_FORMATTER.formatCellValue(cell);
            values.add(cellValue);
        }
        return values;
    }
}
