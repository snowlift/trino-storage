package org.ebyhr.presto.flex.operator;

import io.prestosql.spi.connector.SchemaNotFoundException;
import org.apache.commons.lang3.EnumUtils;
import org.ebyhr.presto.flex.FileType;

import static org.ebyhr.presto.flex.FileType.CSV;
import static org.ebyhr.presto.flex.FileType.EXCEL;
import static org.ebyhr.presto.flex.FileType.RAW;
import static org.ebyhr.presto.flex.FileType.TSV;
import static org.ebyhr.presto.flex.FileType.TXT;

public class PluginFactory {
    public static FilePlugin create(String typeName)
    {
        if (!EnumUtils.isValidEnum(FileType.class, typeName.toUpperCase())) {
            throw new SchemaNotFoundException(typeName);
        }

        FileType fileType = FileType.valueOf(typeName.toUpperCase());

        if (fileType == CSV) {
            return new CsvPlugin();
        } else if (fileType == TSV) {
            return new TsvPlugin();
        } else if (fileType == TXT) {
            return new TextPlugin();
        } else if (fileType == RAW) {
            return new RawPlugin();
        } else if (fileType == EXCEL) {
            return new ExcelPlugin();
        }
        throw new IllegalArgumentException("The file type is not supported " + typeName);
    }
}
